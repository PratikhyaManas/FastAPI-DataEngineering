from __future__ import annotations

from enum import Enum
from datetime import datetime, timedelta, timezone

import jwt

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings


class Role(str, Enum):
    reader = "reader"
    operator = "operator"
    admin = "admin"


_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_ROLE_HEADER = APIKeyHeader(name="X-Role", auto_error=False)
_BEARER = HTTPBearer(auto_error=False)

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


def create_access_token(subject: str, role: Role) -> tuple[str, int]:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    payload = {
        "sub": subject,
        "role": role.value,
        "exp": int(expire.timestamp()),
        "iat": int(datetime.now(timezone.utc).timestamp()),
    }
    token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return token, settings.access_token_expire_minutes * 60


def _decode_bearer_token(credentials: HTTPAuthorizationCredentials | None = Security(_BEARER)) -> Role | None:
    if credentials is None:
        return None
    token = credentials.credentials
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        role_value = str(payload.get("role", Role.reader.value)).lower()
        return Role(role_value)
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired") from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token") from exc


def require_role(required: Role):
    def _dependency(
        bearer_role: Role | None = Depends(_decode_bearer_token),
        _api_key: str | None = Security(_API_KEY_HEADER),
        role_header: str | None = Security(_ROLE_HEADER),
    ) -> Role:
        # Preferred mode: JWT bearer token with role claim
        if bearer_role is not None:
            role = bearer_role
            if _ROLE_LEVELS[role] < _ROLE_LEVELS[required]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Insufficient role. Required: {required.value}",
                )
            return role

        # Backward-compatible mode: API key + role header
        if _api_key is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing authorization. Provide Bearer token or X-API-Key.",
            )
        _validate_api_key(_api_key)
        role = _extract_role(role_header)
        if _ROLE_LEVELS[role] < _ROLE_LEVELS[required]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role. Required: {required.value}",
            )
        return role

    return _dependency