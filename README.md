# News ETL Pipeline

A production-grade data engineering pipeline that extracts real-time news articles from the NewsAPI, transforms and enriches them with sentiment analysis, loads them into PostgreSQL and Google BigQuery, and models the data with dbt.

## Architecture

```
NewsAPI → Python ETL → PostgreSQL (Docker) → Google BigQuery (Cloud)
                              ↓
                         dbt models
                              ↓
                    Monitoring Dashboard
```

## Tech Stack

| Layer | Technology |
|---|---|
| Extraction | Python, NewsAPI, requests |
| Transformation | Pandas, custom sentiment analysis |
| Loading | PostgreSQL (Docker), Google BigQuery |
| Modeling | dbt (staging → marts) |
| Orchestration | Apache Airflow (Docker) |
| Infrastructure | Docker, Docker Compose |
| Monitoring | Custom Python dashboard |
| Cloud | Google Cloud Platform |

## Features

- **Real-time extraction** — pulls news articles across 5 topics every run
- **Data quality** — validates nulls, deduplicates, rejects malformed records
- **Sentiment analysis** — keyword-based positive/negative/neutral classification
- **3-layer architecture** — raw → clean → summary (medallion pattern)
- **dbt models** — staging, fact, dimension, and aggregation layers with 15+ tests
- **Cloud-ready** — loads clean data directly into BigQuery
- **Monitoring dashboard** — real-time pipeline metrics in the browser

## Project Structure

```
news-etl-pipeline/
├── extract/
│   └── news_extractor.py      # NewsAPI client, deduplication
├── transform/
│   └── news_cleaner.py        # Pandas cleaning, sentiment, quality report
├── ecommerce_dbt/
│   ├── models/
│   │   ├── staging/           # stg_orders — typed, renamed views
│   │   └── marts/             # fct_orders, dim_customers, agg_daily_sales
│   └── schema.yml             # dbt tests and documentation
├── run_news_pipeline.py       # Main pipeline runner
├── load_to_bigquery.py        # GCP BigQuery loader
├── dashboard.py               # Browser-based monitoring UI
└── docker-compose.yml         # PostgreSQL + Airflow
```

## Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop
- NewsAPI key (free at newsapi.org)
- Google Cloud account (optional, for BigQuery)

### Setup

```bash
# Clone the repository
git clone https://github.com/BehlulovIsmayil/news-etl-pipeline.git
cd news-etl-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install pandas psycopg2-binary faker sqlalchemy requests python-dotenv google-cloud-bigquery

# Configure environment
cp .env.example .env
# Edit .env and add your NEWS_API_KEY
```

### Run

```bash
# Start PostgreSQL
docker compose up postgres -d

# Run full pipeline
python run_news_pipeline.py

# Launch monitoring dashboard
python dashboard.py
# Open http://localhost:5050
```

### dbt Models

```bash
cd ecommerce_dbt
dbt run    # Build all models
dbt test   # Run 15+ data quality tests
dbt docs generate && dbt docs serve  # Generate documentation
```

## Data Models

### Staging Layer
- `stg_orders` — typed and renamed view over clean source data

### Marts Layer
- `fct_orders` — fact table with all KPIs per order
- `dim_customers` — customer dimension with LTV and segmentation
- `agg_daily_sales` — daily aggregations by category for BI tools

### News Pipeline Tables
- `raw_news` — append-only raw articles (audit trail)
- `clean_news` — validated, typed, sentiment-enriched articles
- `news_summary` — daily aggregations by topic

## Pipeline Metrics (sample run)

```
Raw articles   : 91
Rejected       : 0  (0.0%)
Clean articles : 91
Sentiment      : neutral 79 | positive 8 | negative 4
BigQuery rows  : 91
```

## What I Learned

- Building end-to-end ETL pipelines with Python and Pandas
- Containerizing data infrastructure with Docker
- Data modeling with dbt (staging, facts, dimensions, tests)
- Loading data to cloud data warehouses (BigQuery)
- Monitoring pipeline health with custom dashboards

## Author

**Ismayil Behlulov** — SQL Developer transitioning to Data Engineering

- LinkedIn: [linkedin.com/in/ismayil-behlul-55a37915a](https://www.linkedin.com/in/ismay%C4%B1l-behlul-55a37915a/)
- GitHub: [github.com/BehlulovIsmayil](https://github.com/BehlulovIsmayil)
- Location: Baku, Azerbaijan (open to remote)
