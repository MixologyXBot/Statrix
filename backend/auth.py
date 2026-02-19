# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import base64
import hashlib
import uuid as _uuid
from datetime import datetime, timedelta
from threading import Lock

import bcrypt
from jose import JWTError, jwt

from .config import settings
from .utils.time import utcnow


def _prehash_password(password: str) -> bytes:
    """Pre-hash password with SHA-256 to bypass bcrypt's 72-byte limit."""
    return base64.b64encode(hashlib.sha256(password.encode("utf-8")).digest())


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(
        _prehash_password(plain_password), hashed_password.encode("utf-8")
    )


def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(_prehash_password(password), bcrypt.gensalt()).decode("utf-8")


_denylist: dict[str, datetime] = {}
_denylist_lock = Lock()


def _prune_denylist() -> None:
    """Remove expired entries (call while holding the lock)."""
    now = utcnow()
    expired = [jti for jti, exp in _denylist.items() if exp <= now]
    for jti in expired:
        _denylist.pop(jti, None)


def revoke_token(jti: str, expires_at: datetime) -> None:
    with _denylist_lock:
        _denylist[jti] = expires_at
        _prune_denylist()


def is_token_revoked(jti: str) -> bool:
    with _denylist_lock:
        _prune_denylist()
        return jti in _denylist


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create a JWT access token with a unique jti claim."""
    to_encode = data.copy()
    if expires_delta:
        expire = utcnow() + expires_delta
    else:
        expire = utcnow() + timedelta(hours=settings.JWT_EXPIRE_HOURS)
    to_encode.update({
        "exp": expire,
        "jti": str(_uuid.uuid4()),
    })
    encoded_jwt = jwt.encode(
        to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM
    )
    return encoded_jwt


def verify_token(token: str, credentials_exception: Exception) -> dict[str, str] | None:
    """
    Verify a JWT token and return the payload.
    Raises credentials_exception if token is invalid or revoked.
    """
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        jti: str | None = payload.get("jti")
        if jti and is_token_revoked(jti):
            raise credentials_exception
        return {"email": email, "jti": jti, "exp": payload.get("exp")}
    except JWTError:
        raise credentials_exception
