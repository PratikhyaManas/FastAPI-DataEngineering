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

## Incremental pipeline mode

Watermark-based incremental endpoints process only records newer than each stage watermark:

- `POST /pipeline/run/bronze/incremental`
- `POST /pipeline/run/silver/incremental`
- `POST /pipeline/run/gold/incremental`
- `POST /pipeline/run/full/incremental`

Async incremental variants:

- `POST /pipeline/run/{stage}/incremental/async` where `{stage}` is `bronze|silver|gold|full`

Example:

```bash
curl -X POST "http://127.0.0.1:8000/pipeline/run/full/incremental" \
	-H "X-API-Key: change-me" \
	-H "X-Role: operator"
```

When there is no new data since watermark, stage responses return `records_in=0`, `records_out=0` and a `message` field.

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
- `bandit -r app -lll` (Python SAST)
- `pytest -q`
- `docker build`
- `CodeQL` analysis and security alerts

## CD (GitHub Actions)

Deployment workflow: `.github/workflows/cd.yml`

It triggers on:

- successful completion of CI on `main/master`
- manual `workflow_dispatch`

It performs:

- build and push Docker image to GHCR
- deploy container to Azure Web App for Containers
- apply app settings (`WEBSITES_PORT`, `DATABASE_URL`, `PIPELINE_API_KEY`)

Required GitHub repository secrets:

- `AZURE_CREDENTIALS` (service principal JSON from `az ad sp create-for-rbac`)
- `AZURE_WEBAPP_NAME`
- `DATABASE_URL`
- `PIPELINE_API_KEY`

## CI/CD (Azure DevOps)

Pipeline file: `azure-pipelines.yml`

Stages:

- `CI`: ruff, bandit SAST, pytest, Docker build+push to ACR
- `CD`: deploy image to Azure Web App for Containers + app settings

Set these Azure DevOps pipeline variables (or variable group):

- `dockerRegistryServiceConnection` (ACR service connection name)
- `azureSubscriptionServiceConnection` (Azure RM service connection)
- `acrLoginServer` (for example: `myacr.azurecr.io`)
- `webAppName`
- `DATABASE_URL` (secret)
- `PIPELINE_API_KEY` (secret)
