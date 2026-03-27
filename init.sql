-- news_init.sql — Xəbər pipeline cədvəlləri
-- Mövcud ecommerce_dw bazasına əlavə edilir

CREATE TABLE IF NOT EXISTS raw_news (
    id              SERIAL PRIMARY KEY,
    article_id      TEXT,
    topic           TEXT,
    source_name     TEXT,
    author          TEXT,
    title           TEXT,
    description     TEXT,
    url             TEXT,
    published_at    TEXT,
    content         TEXT,
    fetched_at      TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clean_news (
    article_id              TEXT PRIMARY KEY,
    topic                   TEXT NOT NULL,
    source_name             TEXT,
    author                  TEXT,
    title                   TEXT NOT NULL,
    description             TEXT,
    url                     TEXT,
    published_at            TIMESTAMPTZ,
    published_date          DATE,
    published_hour          INTEGER,
    title_word_count        INTEGER,
    description_word_count  INTEGER,
    sentiment               TEXT CHECK (sentiment IN ('positive','negative','neutral')),
    fetched_at              TEXT,
    loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_summary (
    summary_date     DATE NOT NULL,
    topic            TEXT NOT NULL,
    total_articles   INTEGER,
    positive_count   INTEGER,
    negative_count   INTEGER,
    neutral_count    INTEGER,
    top_source       TEXT,
    avg_title_words  NUMERIC(5,1),
    refreshed_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (summary_date, topic)
);

CREATE INDEX IF NOT EXISTS idx_clean_news_date      ON clean_news(published_date);
CREATE INDEX IF NOT EXISTS idx_clean_news_topic     ON clean_news(topic);
CREATE INDEX IF NOT EXISTS idx_clean_news_sentiment ON clean_news(sentiment);
