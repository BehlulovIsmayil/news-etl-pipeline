"""
extract/news_extractor.py — NewsAPI-dan xəbərləri çəkir.

Hər çağırışda:
  - Seçilmiş mövzular üzrə son 24 saatın xəbərlərini çəkir
  - data/raw/news_YYYYMMDD_HHMMSS.csv faylına yazır
  - Əsas sahələri normallaşdırır
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

API_KEY  = os.getenv("NEWS_API_KEY")
BASE_URL = "https://newsapi.org/v2/everything"

# Axtarılacaq mövzular — istədiyiniz kimi dəyişə bilərsiniz
TOPICS = [
    "artificial intelligence",
    "data engineering",
    "python programming",
    "technology",
    "startup",
]

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_topic(topic: str, from_date: str, page_size: int = 20) -> list[dict]:
    """Bir mövzu üçün NewsAPI-dan xəbər çəkir."""
    params = {
        "q":        topic,
        "from":     from_date,
        "sortBy":   "publishedAt",
        "language": "en",
        "pageSize": page_size,
        "apiKey":   API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "ok":
        log.warning("API xətası [%s]: %s", topic, data.get("message"))
        return []

    articles = data.get("articles", [])
    log.info("  Topic '%s': %d xəbər çəkildi", topic, len(articles))
    return articles


def extract_news(run_ts: str = None) -> Path:
    """
    Bütün mövzular üzrə xəbərləri çəkib CSV-ə yazır.
    Returns: raw CSV faylının yolu
    """
    if not API_KEY:
        raise ValueError("NEWS_API_KEY təyin edilməyib! .env faylını yoxlayın.")

    if run_ts is None:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Son 7 günün xəbərləri
    from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")

    log.info("NewsAPI extract başladı — mövzu sayı: %d", len(TOPICS))
    all_articles = []

    for topic in TOPICS:
        try:
            articles = fetch_topic(topic, from_date)
            for art in articles:
                all_articles.append({
                    "article_id":    art.get("url", ""),
                    "topic":         topic,
                    "source_name":   art.get("source", {}).get("name", ""),
                    "author":        art.get("author", ""),
                    "title":         art.get("title", ""),
                    "description":   art.get("description", ""),
                    "url":           art.get("url", ""),
                    "published_at":  art.get("publishedAt", ""),
                    "content":       art.get("content", ""),
                    "fetched_at":    datetime.now().isoformat(),
                })
        except Exception as e:
            log.warning("Topic '%s' xətası: %s", topic, e)
            continue

    if not all_articles:
        raise RuntimeError("Heç bir xəbər çəkilmədi!")

    df = pd.DataFrame(all_articles)

    # Duplikatları sil (eyni URL fərqli mövzularda çıxa bilər)
    before = len(df)
    df = df.drop_duplicates(subset=["article_id"])
    log.info("Dedup: %d → %d (-%d)", before, len(df), before - len(df))

    out_path = RAW_DIR / f"news_{run_ts}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info("Extract tamamlandı: %d xəbər → %s", len(df), out_path)

    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    path = extract_news()
    df = pd.read_csv(path)
    print(f"\nİlk 3 xəbər:\n")
    for _, row in df.head(3).iterrows():
        print(f"  [{row['topic']}] {row['title'][:80]}")
        print(f"  {row['source_name']} — {row['published_at'][:10]}\n")
