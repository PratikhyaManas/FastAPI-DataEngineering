# FastAPI Data Engineering Pipeline API

This repository is a production-style FastAPI starter for data engineers, inspired by medallion architecture patterns:

- Raw ingestion
- Bronze validation and quarantine
- Silver enrichment
- Gold aggregation
- Partitioned export
- Health and DQ visibility
- API key auth + role-based access control
- Async pipeline job execution and status polling
- PostgreSQL-ready persistence via SQLAlchemy
- GitHub Actions CI (lint, tests, docker build)

## Project structure

```text
.
├── app
│   ├── models
│   ├── routers
│   ├── services
│   ├── config.py
│   ├── logging_config.py
│   └── store.py
├── tests
├── main.py
├── requirements.txt
└── Dockerfile
```

## Run locally

```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --reload --port 8000
```

Open:

- Swagger UI: <http://127.0.0.1:8000/docs>
- ReDoc: <http://127.0.0.1:8000/redoc>

## Typical API flow

1. Ingest sample records:

```bash
curl -X POST "http://127.0.0.1:8000/ingest/sample/stocks?days=5&inject_errors=true"
```

1. Run full pipeline:

```bash
curl -X POST "http://127.0.0.1:8000/pipeline/run/full" \
	-H "X-API-Key: change-me" \
	-H "X-Role: operator"
```

1. Inspect layers:

```bash
curl "http://127.0.0.1:8000/bronze/stocks?valid_only=true"
curl "http://127.0.0.1:8000/silver/stocks"
curl "http://127.0.0.1:8000/gold/stocks"
```

1. Export partitioned files:

```bash
curl -X POST "http://127.0.0.1:8000/export/all"
```

## Security

Pipeline trigger and reset endpoints are protected:

- `X-API-Key`: must match `PIPELINE_API_KEY`
- `X-Role`: `reader`, `operator`, or `admin`

Role requirements:

- `operator`: `/pipeline/run/*` and `/pipeline/run/*/async`
- `admin`: `/health/reset`

## Async jobs

Trigger async stage run:

```bash
curl -X POST "http://127.0.0.1:8000/pipeline/run/bronze/async" \
	-H "X-API-Key: change-me" \
	-H "X-Role: operator"
```

Poll job status:

```bash
curl "http://127.0.0.1:8000/pipeline/status/<job_id>"
```

## Database

Default local DB uses SQLite for easy startup:

```env
DATABASE_URL=sqlite:///./pipeline.db
```

Use PostgreSQL in development/production by setting:

```env
DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/pipeline_db
```

## Run tests

```bash
pytest -q
```

## Container run

```bash
docker build -t fastapi-de-pipeline .
docker run --rm -p 8000:8000 fastapi-de-pipeline
```

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` runs:

- `ruff check .`
- `pytest -q`
- `docker build`
