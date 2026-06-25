import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal, init_db, init_pipeline_state  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def bootstrap_db():
    init_db()
    db = SessionLocal()
    try:
        init_pipeline_state(db)
    finally:
        db.close()
