"""
KeycloakHook — lay access_token qua luong ROPC (Resource Owner Password
Credentials) de xac thuc voi pipeline-api.

Connection `keycloak_default`:
    host     : base URL cua Keycloak, vd http://keycloak:8080
    login    : username service account (vd airflow)
    password : mat khau service account
    extra    : {"realm": "lakehouse",
                "client_id": "pipeline-runner",
                "client_secret": "pipeline-runner-secret"}
"""
from __future__ import annotations

import time

import requests
from airflow.hooks.base import BaseHook

DEFAULT_CONN_ID = "keycloak_default"
# Lan xin token dau tien sau khi Keycloak dev-mode khoi dong co the mat
# 20-30 giay de khoi tao password hashing/cache tren may local tai cao.
_TIMEOUT = 60
# Xin token moi som hon han thuc te de tranh dung ngay bien gioi het han
_EXPIRY_SKEW = 30


class KeycloakHook(BaseHook):
    """Hook toi Keycloak, cache token trong pham vi 1 instance."""
    def __init__(self, conn_id: str = DEFAULT_CONN_ID) -> None:
        super().__init__()
        self.conn_id = conn_id
        self._token: str | None = None
        self._expires_at: float = 0.0

    def _cfg(self) -> dict:
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        base = (conn.host or "").rstrip("/")
        if conn.port:
            base = f"{base}:{conn.port}"
        return {
            "token_url": (
                f"{base}/realms/{extra['realm']}"
                "/protocol/openid-connect/token"
            ),
            "client_id": extra["client_id"],
            "client_secret": extra.get("client_secret", ""),
            "username": conn.login,
            "password": conn.password,
        }

    def get_token(self, force: bool = False) -> str:
        """Tra ve access_token con hieu luc (tu cache neu chua het han)."""
        if not force and self._token and time.time() < self._expires_at:
            return self._token

        cfg = self._cfg()
        resp = requests.post(
            cfg["token_url"],
            data={
                "grant_type": "password",
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "username": cfg["username"],
                "password": cfg["password"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._expires_at = time.time() + int(payload.get("expires_in", 300)) - _EXPIRY_SKEW
        self.log.info("Keycloak: lay token ROPC thanh cong (client=%s)", cfg["client_id"])
        return self._token
