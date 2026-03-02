# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .auth import verify_token
from .database import db

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token = credentials.credentials
    payload = verify_token(token, credentials_exception)

    if payload is None:
        raise credentials_exception

    user = await db.get_user_by_email(payload["email"])
    if user is None:
        raise credentials_exception

    user = dict(user)
    user["_jti"] = payload.get("jti")
    user["_exp"] = payload.get("exp")
    return user


async def get_current_admin(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required"
        )
    return current_user
