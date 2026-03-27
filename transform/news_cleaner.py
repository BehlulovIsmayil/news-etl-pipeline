"""
transform/news_cleaner.py — Xəbər datasını təmizləyir və zənginləşdirir.

Addımlar:
  1. Null/boş başlıqları rədd et
  2. Tarix tipini çevir
  3. Mətn uzunluğunu hesabla
  4. Sadə keyword-based sentiment təyin et
  5. Dublikat URL-ləri sil
"""

import re
import logging
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

CLEAN_DIR = Path("data/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# Sadə keyword-based sentiment
POSITIVE_WORDS = {
    "growth", "success", "innovation", "breakthrough", "launch", "profit",
    "win", "achieve", "improve", "gain", "rise", "surge", "record", "best",
    "leading", "advance", "revolutionize", "opportunity", "partnership"
}
NEGATIVE_WORDS = {
    "crash", "fail", "loss", "decline", "risk", "threat", "warn", "drop",
    "cut", "layoff", "bankrupt", "sue", "hack", "breach", "scandal",
    "crisis", "collapse", "fraud", "concern", "problem"
}


def simple_sentiment(text: str) -> str:
    """Sadə keyword-based sentiment: positive / negative / neutral."""
    if not text or pd.isna(text):
        return "neutral"
    words  = set(re.findall(r'\b\w+\b', text.lower()))
    pos    = len(words & POSITIVE_WORDS)
    neg    = len(words & NEGATIVE_WORDS)
    if pos > neg:   return "positive"
    if neg > pos:   return "negative"
    return "neutral"


def word_count(text: str) -> int:
    if not text or pd.isna(text): return 0
    return len(str(text).split())


@dataclass
class NewsQualityReport:
    raw_rows:      int = 0
    clean_rows:    int = 0
    rejected_rows: int = 0
    issues:        dict = field(default_factory=dict)

    def log_summary(self):
        log.info("── News Quality Report ─────────────────")
        log.info("  Raw rows    : %d", self.raw_rows)
        log.info("  Clean rows  : %d", self.clean_rows)
        log.info("  Rejected    : %d", self.rejected_rows)
        for k, v in self.issues.items():
            log.info("    %-25s %d", k + ":", v)
        log.info("────────────────────────────────────────")


def transform_news(raw_path: Path) -> tuple[pd.DataFrame, NewsQualityReport]:
    """Raw xəbər CSV-ini oxuyub təmizləyir."""
    log.info("Transform: %s", raw_path)
    df     = pd.read_csv(raw_path)
    report = NewsQualityReport(raw_rows=len(df))

    # 1. Boş başlıqları rədd et
    null_title = df["title"].isna() | (df["title"].str.strip() == "") | (df["title"] == "[Removed]")
    # 2. Boş URL-ləri rədd et
    null_url   = df["article_id"].isna() | (df["article_id"].str.strip() == "")
    # 3. Dublikat URL
    dup_url    = df.duplicated(subset=["article_id"], keep="first")

    report.issues["null_title"]  = int(null_title.sum())
    report.issues["null_url"]    = int(null_url.sum())
    report.issues["dup_url"]     = int(dup_url.sum())

    reject_mask = null_title | null_url | dup_url
    clean_df    = df[~reject_mask].copy()

    # 4. Tarix çevrilməsi
    clean_df["published_at"] = pd.to_datetime(
        clean_df["published_at"], errors="coerce", utc=True
    )
    clean_df["published_date"] = clean_df["published_at"].dt.date
    clean_df["published_hour"] = clean_df["published_at"].dt.hour

    # 5. Mətn zənginləşdirməsi
    clean_df["title_word_count"]       = clean_df["title"].apply(word_count)
    clean_df["description_word_count"] = clean_df["description"].apply(word_count)

    # 6. Sentiment
    clean_df["sentiment"] = clean_df["title"].apply(simple_sentiment)

    # 7. Mənbə normallaşdırması
    clean_df["source_name"] = clean_df["source_name"].str.strip().str.title()
    clean_df["topic"]       = clean_df["topic"].str.strip().str.lower()

    report.clean_rows    = len(clean_df)
    report.rejected_rows = report.raw_rows - report.clean_rows
    report.log_summary()

    return clean_df, report


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    raw_files = sorted(Path("data/raw").glob("news_*.csv"))
    if not raw_files:
        print("Raw xəbər faylı yoxdur. Əvvəlcə: python extract/news_extractor.py")
        sys.exit(1)

    clean_df, report = transform_news(raw_files[-1])
    print(f"\nSentiment paylanması:")
    print(clean_df["sentiment"].value_counts().to_string())
    print(f"\nMövzu paylanması:")
    print(clean_df["topic"].value_counts().to_string())
