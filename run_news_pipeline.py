"""
run_news_pipeline.py — NewsAPI ETL pipeline runner

Extract (NewsAPI) → Transform (təmizlə + sentiment) → Load (PostgreSQL)
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/news_{datetime.now():%Y%m%d}.log"),
    ],
)
log = logging.getLogger("news_pipeline")

Path("logs").mkdir(exist_ok=True)


def run():
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("=" * 55)
    log.info("  NewsAPI ETL Pipeline  run_ts=%s", run_ts)
    log.info("=" * 55)

    # ── EXTRACT ──────────────────────────────────────────
    log.info("[1/3] EXTRACT — NewsAPI-dan xəbərlər çəkilir...")
    from extract.news_extractor import extract_news
    raw_path = extract_news(run_ts=run_ts)

    # ── TRANSFORM ────────────────────────────────────────
    log.info("[2/3] TRANSFORM — təmizlənir, sentiment hesablanır...")
    from transform.news_cleaner import transform_news
    clean_df, report = transform_news(raw_path)

    clean_path = Path("data/clean") / f"news_clean_{run_ts}.csv"
    clean_df.to_csv(clean_path, index=False, encoding="utf-8")

    # ── LOAD ─────────────────────────────────────────────
    log.info("[3/3] LOAD — PostgreSQL-ə yazılır...")
    import psycopg2
    import psycopg2.extras
    import pandas as pd

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=os.getenv("DB_NAME", "ecommerce_dw"),
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASSWORD", "etl_pass"),
    )

    try:
        # Raw insert
        raw_df   = pd.read_csv(raw_path, dtype=str)
        raw_cols = ["article_id","topic","source_name","author",
                    "title","description","url","published_at","content","fetched_at"]
        raw_recs = raw_df[[c for c in raw_cols if c in raw_df.columns]].where(
            pd.notnull(raw_df), None).to_dict("records")

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO raw_news (article_id,topic,source_name,author,
                    title,description,url,published_at,content,fetched_at)
                VALUES (%(article_id)s,%(topic)s,%(source_name)s,%(author)s,
                    %(title)s,%(description)s,%(url)s,%(published_at)s,
                    %(content)s,%(fetched_at)s)
            """, raw_recs, page_size=200)

        # Clean upsert
        clean_cols = ["article_id","topic","source_name","author","title",
                      "description","url","published_at","published_date",
                      "published_hour","title_word_count","description_word_count",
                      "sentiment","fetched_at"]
        df = clean_df[[c for c in clean_cols if c in clean_df.columns]].copy()
        df["published_at"]   = df["published_at"].astype(str)
        df["published_date"] = df["published_date"].astype(str)
        recs = df.where(pd.notnull(df), None).to_dict("records")

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO clean_news (article_id,topic,source_name,author,title,
                    description,url,published_at,published_date,published_hour,
                    title_word_count,description_word_count,sentiment,fetched_at)
                VALUES (%(article_id)s,%(topic)s,%(source_name)s,%(author)s,%(title)s,
                    %(description)s,%(url)s,%(published_at)s,%(published_date)s,
                    %(published_hour)s,%(title_word_count)s,%(description_word_count)s,
                    %(sentiment)s,%(fetched_at)s)
                ON CONFLICT (article_id) DO UPDATE SET
                    sentiment=EXCLUDED.sentiment, loaded_at=NOW()
            """, recs, page_size=200)

        # Summary refresh
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO news_summary
                    (summary_date, topic, total_articles, positive_count,
                     negative_count, neutral_count, top_source, avg_title_words)
                SELECT
                    published_date,
                    topic,
                    COUNT(*)                                          AS total_articles,
                    COUNT(*) FILTER (WHERE sentiment='positive')     AS positive_count,
                    COUNT(*) FILTER (WHERE sentiment='negative')     AS negative_count,
                    COUNT(*) FILTER (WHERE sentiment='neutral')      AS neutral_count,
                    mode() WITHIN GROUP (ORDER BY source_name)       AS top_source,
                    ROUND(AVG(title_word_count),1)                   AS avg_title_words
                FROM clean_news
                WHERE published_date IS NOT NULL
                GROUP BY published_date, topic
                ON CONFLICT (summary_date, topic) DO UPDATE SET
                    total_articles = EXCLUDED.total_articles,
                    positive_count = EXCLUDED.positive_count,
                    negative_count = EXCLUDED.negative_count,
                    neutral_count  = EXCLUDED.neutral_count,
                    top_source     = EXCLUDED.top_source,
                    avg_title_words= EXCLUDED.avg_title_words,
                    refreshed_at   = NOW()
            """)

        conn.commit()
        log.info("Load tamamlandı.")

    except Exception as e:
        conn.rollback()
        log.error("Load xətası: %s", e)
        raise
    finally:
        conn.close()

    # ── SUMMARY ──────────────────────────────────────────
    log.info("=" * 55)
    log.info("  Pipeline complete ✓")
    log.info("  Raw xəbərlər  : %d", report.raw_rows)
    log.info("  Rədd edildi   : %d", report.rejected_rows)
    log.info("  Clean xəbər   : %d", report.clean_rows)
    log.info("  Sentiment:")
    for s, c in clean_df["sentiment"].value_counts().items():
        log.info("    %-10s %d", s, c)
    log.info("=" * 55)


if __name__ == "__main__":
    run()
