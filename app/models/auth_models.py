from pydantic import BaseModel, Field


class TokenRequest(BaseModel):
    username: str = Field(min_length=1)
    role: str = Field(default="reader")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int
