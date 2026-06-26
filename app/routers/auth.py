from fastapi import APIRouter, HTTPException

from app.models.auth_models import TokenRequest, TokenResponse
from app.security import Role, create_access_token

router = APIRouter()


@router.post("/token", response_model=TokenResponse)
def issue_token(payload: TokenRequest):
    role_value = payload.role.strip().lower()
    try:
        role = Role(role_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid role. Use reader|operator|admin") from exc

    token, expires = create_access_token(subject=payload.username, role=role)
    return TokenResponse(access_token=token, expires_in_seconds=expires)
