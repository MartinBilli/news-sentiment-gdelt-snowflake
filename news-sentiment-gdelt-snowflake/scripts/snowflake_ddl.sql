-- Schémata
CREATE SCHEMA IF NOT EXISTS NEWS_RAW;
CREATE SCHEMA IF NOT EXISTS NEWS_MART;

-- Detailní tabulka
CREATE TABLE IF NOT EXISTS NEWS_RAW.NEWS_GKG_ARTICLES (
  article_id STRING,
  published_at TIMESTAMP_NTZ,
  date_key DATE,
  country_code_fips STRING,
  source_domain STRING,
  source_name STRING,
  language STRING,
  title STRING,
  url STRING,
  gdelt_tone FLOAT,
  tone_positive INT,
  tone_negative INT,
  tone_neutral INT,
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Denní agregace (FIPS kódy)
CREATE TABLE IF NOT EXISTS NEWS_MART.NEWS_DAILY_SENTIMENT (
  dt DATE,
  country_code_fips STRING,
  pos_count INT,
  neg_count INT,
  neu_count INT,
  avg_tone FLOAT,
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
