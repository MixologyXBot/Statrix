# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import time

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ..auth import create_access_token, revoke_token, verify_password
from ..database import db
from ..dependencies import get_current_user
from ..models import LoginRequest, TokenResponse, UserResponse
from ..utils.time import utcnow

router = APIRouter()

_MAX_LOGIN_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 300  # 5 minutes
_login_attempts: dict[str, list[float]] = defaultdict(list)


def _check_login_rate_limit(ip: str) -> None:
    now = time()
    _login_attempts[ip] = [
        t for t in _login_attempts[ip] if now - t < _LOGIN_WINDOW_SECONDS
    ]
    if len(_login_attempts[ip]) >= _MAX_LOGIN_ATTEMPTS:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Please try again later.",
        )
    _login_attempts[ip].append(now)


@router.post("/login", response_model=TokenResponse)
async def login(request_body: LoginRequest, request: Request):
    client_ip = request.client.host if request.client else "unknown"
    _check_login_rate_limit(client_ip)

    user = await db.get_user_by_email(request_body.email)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not verify_password(request_body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    access_token = create_access_token(data={"sub": user["email"]})

    return TokenResponse(access_token=access_token)


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserResponse(**current_user)


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    jti = current_user.get("_jti")
    exp = current_user.get("_exp")
    if jti and exp:
        # exp is a Unix timestamp from the JWT
        try:
            expires_at = datetime.fromtimestamp(float(exp), tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, TypeError, OSError):
            expires_at = utcnow() + timedelta(hours=1)
        revoke_token(jti, expires_at)
    return {"message": "Logged out successfully"}
