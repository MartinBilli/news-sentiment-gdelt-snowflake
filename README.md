# news-sentiment-gdelt-snowflake
Sber zprav v deleni na pozitivni a negativni v ramci EU
Denní pipeline pro získání počtu **pozitivních/negativních** zpráv podle **zemí** z **GDELT GKG v2** a uložení agregací do **Snowflake**. Běží přes **GitHub Actions** (cron).

## Co dělá
- Každý den stáhne 15min GDELT GKG soubory za včerejšek.
- Spočítá sentiment z pole **V2Tone** (pozitivní/negativní/neutral dle prahů).
- Určí zemi dvěma způsoby:
  1. z **TLD** domény (např. `.cz -> CZ`) u webových zdrojů (SourceCollectionIdentifier=1),
  2. z pole **V2Locations** – vezme první dostupný kód země (**FIPS10-4**) v bloku lokace.
- Uloží **detail** do `NEWS_RAW.NEWS_GKG_ARTICLES` a **denní agregaci** do `NEWS_MART.NEWS_DAILY_SENTIMENT`.

> Pozn.: GDELT GKG používá kódy **FIPS10-4** u lokací; mapování na ISO 3166 lze doplnit později přes referenční tabulku. Viz oficiální **GKG v2 codebook**. citeturn4search1 citeturn4search7

## Rychlý start
1. **Vytvoř GitHub repo** (prázdné). Název může být `news-sentiment-gdelt-snowflake`.
2. Stáhni tento balíček (ZIP), rozbal a pushni do repo:
   ```bash
   git init
   git remote add origin <URL_tveho_repa>
   git add .
   git commit -m "Initial: GDELT→Snowflake daily sentiment"
   git branch -M main
   git push -u origin main
   ```
3. V **Settings → Secrets and variables → Actions → New repository secret** nastav:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`

4. Vytvoř tabulky v Snowflake (viz `scripts/snowflake_ddl.sql`).
5. (Volitelné) Uprav prahy sentimentu v workflow ENV: `SENTIMENT_POS_THRESHOLD`, `SENTIMENT_NEG_THRESHOLD`.
6. Spusť ručně workflow (**Actions → Run workflow**), nebo počkej na denní cron.

## Architektura
- **Zdroj**: GDELT Global Knowledge Graph (GKG) v2, 15min soubory: `YYYYMMDDHHMM.gkg.csv(.zip)` v adresáři `http://data.gdeltproject.org/gkg/`. GDELT je veřejně dostupný a zdarma. citeturn4search9
- **Schema pole V2Tone**: první číslo je průměrný **Tone** v rozsahu -100..+100; další čísla zahrnují Positive/Negative Score atd. (comma-delimited). citeturn4search7
- **Pole V2Locations**: obsahuje bloky lokací včetně **CountryCode (FIPS10-4)**; používáme první nalezený kód pro odvození země. citeturn4search7

## Vizualizace
- Napoj `NEWS_MART.NEWS_DAILY_SENTIMENT` do Power BI a vytvoř line chart (`dt` vs. `pos_count`/`neg_count`; slicer `country_code_fips`).

## Licence
MIT © 2026 Martin Vorel
