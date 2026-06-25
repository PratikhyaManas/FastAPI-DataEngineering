from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.database import SessionLocal, init_db, init_pipeline_state
from app.logging_config import logger
from app.routers import bronze, export, gold, health, ingest, pipeline, silver


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    db: Session = SessionLocal()
    try:
        init_pipeline_state(db)
    finally:
        db.close()
    yield

app = FastAPI(
    title="Data Pipeline API",
    description="Production-style FastAPI service for medallion data pipelines.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    response = await call_next(request)
    logger.info(
        "method=%s path=%s status=%s",
        request.method,
        request.url.path,
        response.status_code,
    )
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_pipeline_error",
            "message": "Unexpected error occurred. Check logs for details.",
            "path": str(request.url.path),
        },
    )


app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(ingest.router, prefix="/ingest", tags=["Ingestion"])
app.include_router(pipeline.router, prefix="/pipeline", tags=["Pipeline"])
app.include_router(bronze.router, prefix="/bronze", tags=["Bronze"])
app.include_router(silver.router, prefix="/silver", tags=["Silver"])
app.include_router(gold.router, prefix="/gold", tags=["Gold"])
app.include_router(export.router, prefix="/export", tags=["Export"])
