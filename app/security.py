from __future__ import annotations

from enum import Enum

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import settings


class Role(str, Enum):
    reader = "reader"
    operator = "operator"
    admin = "admin"


_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_ROLE_HEADER = APIKeyHeader(name="X-Role", auto_error=False)

_ROLE_LEVELS = {
    Role.reader: 0,
    Role.operator: 1,
    Role.admin: 2,
}


def _validate_api_key(api_key: str | None = Security(_API_KEY_HEADER)) -> str:
    expected = settings.pipeline_api_key
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="PIPELINE_API_KEY is not configured.",
        )
    if api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key.",
        )
    return api_key


def _extract_role(role_header: str | None = Security(_ROLE_HEADER)) -> Role:
    value = (role_header or "").strip().lower() or Role.reader.value
    try:
        return Role(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid X-Role header value.") from exc


def require_role(required: Role):
    def _dependency(_api_key: str = Depends(_validate_api_key), role: Role = Depends(_extract_role)) -> Role:
        if _ROLE_LEVELS[role] < _ROLE_LEVELS[required]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role. Required: {required.value}",
            )
        return role

    return _dependency