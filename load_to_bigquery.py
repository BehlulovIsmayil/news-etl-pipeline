"""
load_to_bigquery.py — Clean xəbər datasını BigQuery-ə yükləyir.

İstifadə:
    python load_to_bigquery.py
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bq_loader")

# ── Konfiqurasiya ────────────────────────────────────────────────
PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "news-etl-pipeline")
DATASET_ID  = "news_raw"
TABLE_ID    = "clean_news"
FULL_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def load_to_bq():
    from google.cloud import bigquery
    import pandas as pd

    # Son clean CSV faylını tap
    clean_files = sorted(Path("data/clean").glob("news_clean_*.csv"))
    if not clean_files:
        raise FileNotFoundError(
            "Clean CSV tapılmadı. Əvvəlcə: python run_news_pipeline.py"
        )

    clean_path = clean_files[-1]
    log.info("Fayl oxunur: %s", clean_path)

    df = pd.read_csv(clean_path)
    log.info("Sətir sayı: %d", len(df))

    # Tip çevrilməsi
    df["published_at"]   = pd.to_datetime(df["published_at"],   errors="coerce", utc=True)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce").dt.date

    # BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Job konfiqurasiyası — mövcud data varsa əlavə et
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    log.info("BigQuery-ə yüklənir: %s", FULL_TABLE)
    job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
    job.result()  # Bitənə qədər gözlə

    table = client.get_table(FULL_TABLE)
    log.info("Uğurlu! Cədvəldə indi %d sətir var.", table.num_rows)
    log.info("BigQuery Console: https://console.cloud.google.com/bigquery?project=%s", PROJECT_ID)


if __name__ == "__main__":
    load_to_bq()
