"""
quorum/api/auth.py
------------------
OAuth2 password-flow authentication with JWT issuance and rotation.

Design notes
------------
- Access tokens are short-lived (settings.ACCESS_TOKEN_EXPIRE_MINUTES).
- Refresh tokens have a longer TTL (settings.REFRESH_TOKEN_EXPIRE_DAYS)
  and include a "type: refresh" claim so they cannot be used as access tokens.
- Passwords are hashed with bcrypt via passlib.
- All token operations use python-jose with the HS256 algorithm by default.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict

from config.settings import settings

# ---------------------------------------------------------------------------
# OAuth2 scheme
# ---------------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")
"""FastAPI dependency that extracts the Bearer token from the Authorization header."""

# ---------------------------------------------------------------------------
# Password hashing context
# ---------------------------------------------------------------------------

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
"""Passlib context for bcrypt password hashing."""

# ---------------------------------------------------------------------------
# Pydantic models (strict v2)
# ---------------------------------------------------------------------------


class Token(BaseModel):
    """Response model returned after successful authentication.

    Contains both an access token (short TTL) and a refresh token (long TTL).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Decoded claims extracted from a validated access token.

    Populated by get_current_user and injected via FastAPI Depends.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    username: Optional[str] = None
    scopes: list[str] = []


class UserInDB(BaseModel):
    """In-memory or DB-backed user representation.

    Extend or replace with a real ORM model as needed.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    username: str
    hashed_password: str
    disabled: bool = False


# ---------------------------------------------------------------------------
# Password utilities
# ---------------------------------------------------------------------------


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash.

    Parameters
    ----------
    plain:
        The plaintext password supplied by the user.
    hashed:
        The bcrypt hash stored in the database.

    Returns
    -------
    bool
        True if the password matches, False otherwise.
    """
    return pwd_context.verify(plain, hashed)


def get_password_hash(password: str) -> str:
    """Hash a plaintext password with bcrypt.

    Parameters
    ----------
    password:
        The plaintext password to hash.

    Returns
    -------
    str
        The bcrypt hash string, suitable for storage.
    """
    return pwd_context.hash(password)


# ---------------------------------------------------------------------------
# Token creation
# ---------------------------------------------------------------------------


def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Create a signed JWT access token.

    The token includes a "type: access" claim to distinguish it from refresh
    tokens at validation time.

    Parameters
    ----------
    data:
        Payload dict to encode. Typically {"sub": username, "scopes": [...]}.
    expires_delta:
        Optional custom expiry. Defaults to
        settings.ACCESS_TOKEN_EXPIRE_MINUTES minutes from now.

    Returns
    -------
    str
        A compact, URL-safe JWT string.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta
        if expires_delta is not None
        else timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict) -> str:
    """Create a signed JWT refresh token with a longer TTL.

    The token includes a "type: refresh" claim so it cannot be used in
    place of an access token. Callers must exchange it for a new access
    token via the /api/v1/auth/refresh endpoint.

    Parameters
    ----------
    data:
        Payload dict to encode. Typically {"sub": username}.

    Returns
    -------
    str
        A compact, URL-safe JWT string.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


async def get_current_user(
    token: str = Depends(oauth2_scheme),
) -> TokenData:
    """FastAPI dependency: validate the Bearer token and return decoded claims.

    Raises HTTPException 401 if the token is missing, expired, malformed,
    or is a refresh token (not an access token).

    Parameters
    ----------
    token:
        JWT string extracted from the Authorization: Bearer <token> header
        by the oauth2_scheme dependency.

    Returns
    -------
    TokenData
        Decoded token claims for use in route handlers.

    Raises
    ------
    HTTPException
        Status 401 with WWW-Authenticate: Bearer if validation fails.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload: dict = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )

        # Reject refresh tokens used as access tokens.
        if payload.get("type") != "access":
            raise credentials_exception

        username: Optional[str] = payload.get("sub")
        if username is None:
            raise credentials_exception

        scopes: list[str] = payload.get("scopes", [])
        return TokenData(username=username, scopes=scopes)

    except JWTError:
        raise credentials_exception
