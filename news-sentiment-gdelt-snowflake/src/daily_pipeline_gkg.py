
import os
import io
import sys
import hashlib
import datetime as dt
from typing import List
import requests
import pandas as pd
import tldextract
from tenacity import retry, stop_after_attempt, wait_exponential
import snowflake.connector

BASE_URL = "http://data.gdeltproject.org/gkg"  # GDELT GKG 2.0 directory
POS_THR = float(os.getenv("SENTIMENT_POS_THRESHOLD", "0.2"))
NEG_THR = float(os.getenv("SENTIMENT_NEG_THRESHOLD", "-0.2"))

SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA_RAW = os.getenv("SNOWFLAKE_SCHEMA_RAW", "NEWS_RAW")
SF_SCHEMA_MART = os.getenv("SNOWFLAKE_SCHEMA_MART", "NEWS_MART")

# Column indices (0-based) per GKG v2.1 codebook
IDX_DATE = 1
IDX_SOURCECOLLECTION = 2
IDX_SOURCECOMMONNAME = 3
IDX_DOCUMENTIDENTIFIER = 4
IDX_V2LOCATIONS = 11
IDX_V2TONE = 16


def yesterday_date() -> dt.date:
    return dt.date.today() - dt.timedelta(days=1)


def generate_gkg_filenames(day: dt.date) -> List[str]:
    names = []
    base = day.strftime("%Y%m%d")
    for h in range(24):
        for q in [0, 15, 30, 45]:
            names.append(f"{base}{h:02d}{q:02d}.gkg.csv")
    return names


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def fetch_file_bytes(url: str) -> bytes:
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return b""
    resp.raise_for_status()
    return resp.content


def read_gkg_to_df(content: bytes, zipped: bool) -> pd.DataFrame:
    if not content:
        return pd.DataFrame()
    if zipped:
        import zipfile
        zf = zipfile.ZipFile(io.BytesIO(content))
        name = zf.namelist()[0]
        with zf.open(name) as f:
            return pd.read_csv(f, sep='	', header=None, engine='python', on_bad_lines='skip', usecols=[IDX_DATE, IDX_SOURCECOLLECTION, IDX_SOURCECOMMONNAME, IDX_DOCUMENTIDENTIFIER, IDX_V2LOCATIONS, IDX_V2TONE])
    else:
        return pd.read_csv(io.BytesIO(content), sep='	', header=None, engine='python', on_bad_lines='skip', usecols=[IDX_DATE, IDX_SOURCECOLLECTION, IDX_SOURCECOMMONNAME, IDX_DOCUMENTIDENTIFIER, IDX_V2LOCATIONS, IDX_V2TONE])


def extract_domain(url: str) -> str:
    if not isinstance(url, str) or not url:
        return None
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return None


def tld_to_country(tld_suffix: str) -> str:
    if not tld_suffix:
        return None
    if len(tld_suffix) == 2 and tld_suffix.isalpha():
        return tld_suffix.upper()
    return None


def country_from_v2locations(v2loc: str) -> str:
    if not isinstance(v2loc, str) or not v2loc:
        return None
    try:
        for block in v2loc.split(';'):
            parts = block.split('#')
            if len(parts) >= 3:
                cc = parts[2].strip()
                if cc:
                    return cc
    except Exception:
        return None
    return None


def parse_tone(v2tone_raw: str) -> float:
    try:
        return float((v2tone_raw or '').split(',')[0])
    except Exception:
        return None


def make_article_id(url: str, ts: str) -> str:
    raw = f"{url}|{ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def sf_connect():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA_RAW,
    )


def insert_detail(conn, df: pd.DataFrame):
    if df.empty:
        return
    insert_cols = [
        "article_id","published_at","date_key","country_code_fips","source_domain",
        "source_name","language","title","url","gdelt_tone",
        "tone_positive","tone_negative","tone_neutral"
    ]
    df2 = df.copy()
    df2["language"] = None
    df2["title"] = None

    rows = df2[[
        "article_id","published_at","date_key","country_code_fips","source_domain",
        "source_name","language","title","url","gdelt_tone",
        "tone_positive","tone_negative","tone_neutral"
    ]].values.tolist()

    cur = conn.cursor()
    sql = f"INSERT INTO {SF_SCHEMA_RAW}.NEWS_GKG_ARTICLES ({','.join(insert_cols)}) VALUES ({','.join(['%s']*len(insert_cols))})"
    cur.executemany(sql, rows)


def insert_daily_agg(conn, df: pd.DataFrame):
    if df.empty:
        return
    agg = (
        df.dropna(subset=["country_code_fips"]).groupby(["date_key","country_code_fips"]).agg(
            pos_count=("tone_positive","sum"),
            neg_count=("tone_negative","sum"),
            neu_count=("tone_neutral","sum"),
            avg_tone=("gdelt_tone","mean")
        ).reset_index()
    )
    agg.rename(columns={"date_key":"dt"}, inplace=True)

    rows = agg[["dt","country_code_fips","pos_count","neg_count","neu_count","avg_tone"]].values.tolist()
    cur = conn.cursor()
    sql2 = "INSERT INTO {schema}.NEWS_DAILY_SENTIMENT (dt, country_code_fips, pos_count, neg_count, neu_count, avg_tone) VALUES (%s,%s,%s,%s,%s,%s)".format(schema=SF_SCHEMA_MART)
    cur.executemany(sql2, rows)


def main():
    day = yesterday_date()
    filenames = generate_gkg_filenames(day)

    frames = []
    for name in filenames:
        url_zip = f"{BASE_URL}/{name}.zip"
        url_csv = f"{BASE_URL}/{name}"
        try:
            content = fetch_file_bytes(url_zip)
            if content:
                df = read_gkg_to_df(content, zipped=True)
            else:
                content2 = fetch_file_bytes(url_csv)
                df = read_gkg_to_df(content2, zipped=False)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"[WARN] {name} failed: {e}", file=sys.stderr)
            continue

    if not frames:
        print(f"[INFO] No GKG data found for {day}")
        return

    df_all = pd.concat(frames, ignore_index=True)
    df_all.columns = ["ts","source_collection","source_name","url","v2locations","v2tone_raw"]

    df_all["gdelt_tone"] = df_all["v2tone_raw"].apply(parse_tone)
    df_all["published_at"] = pd.to_datetime(df_all["ts"], format="%Y%m%d%H%M%S", errors="coerce")
    df_all["date_key"] = df_all["published_at"].dt.date

    df_all["country_code_fips"] = df_all["v2locations"].apply(country_from_v2locations)

    def tld_fallback(row):
        if pd.notna(row.get("country_code_fips")) and row.get("country_code_fips"):
            return row.get("country_code_fips")
        if int(row.get("source_collection") or 0) == 1:
            domain = extract_domain(row.get("url"))
            suffix = domain.split(".")[-1] if isinstance(domain, str) and "." in domain else None
            return tld_to_country(suffix)
        return None

    df_all["country_code_fips"] = df_all.apply(tld_fallback, axis=1)

    df_all["source_domain"] = df_all["url"].apply(extract_domain)

    tone = df_all["gdelt_tone"].fillna(0)
    df_all["tone_positive"] = (tone > POS_THR).astype(int)
    df_all["tone_negative"] = (tone < NEG_THR).astype(int)
    df_all["tone_neutral"] = (~((tone > POS_THR) | (tone < NEG_THR))).astype(int)

    df_all["article_id"] = [make_article_id(u, str(t)) for u, t in zip(df_all["url"], df_all["published_at"])]

    cols = [
        "article_id","published_at","date_key","country_code_fips","source_domain","source_name",
        "url","gdelt_tone","tone_positive","tone_negative","tone_neutral"
    ]
    df_final = df_all[cols].copy()

    conn = sf_connect()
    try:
        insert_detail(conn, df_final)
        insert_daily_agg(conn, df_final)
    finally:
        conn.close()

    print(f"[OK] Inserted {len(df_final)} detail rows and daily aggregation for {day}")


if __name__ == "__main__":
    main()
