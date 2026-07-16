"""
NifiHook — dieu khien process group NiFi qua REST API.

Connection `nifi_default` (NiFi 1.27 single-user, chay HTTPS):
    host     : base URL, vd https://nifi:8443
    login    : NIFI_USER
    password : NIFI_PASSWORD
    extra    : {"verify_ssl": false}   # NiFi single-user dung cert tu ky

Luong xac thuc: POST /nifi-api/access/token (form user/pass) -> JWT (text),
gan vao header Authorization: Bearer <token>.
"""
from __future__ import annotations

import requests
import urllib3
from airflow.hooks.base import BaseHook

DEFAULT_CONN_ID = "nifi_default"
_TIMEOUT = 30


class NifiHook(BaseHook):
    def __init__(self, conn_id: str = DEFAULT_CONN_ID) -> None:
        super().__init__()
        self.conn_id = conn_id
        self._base: str | None = None
        self._verify: bool = True
        self._token: str | None = None

    def _connect(self) -> None:
        if self._base is not None:
            return
        conn = self.get_connection(self.conn_id)
        base = (conn.host or "").rstrip("/")
        if conn.port:
            base = f"{base}:{conn.port}"
        self._base = base
        self._verify = bool(conn.extra_dejson.get("verify_ssl", False))
        if not self._verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.post(
            f"{self._base}/nifi-api/access/token",
            data={"username": conn.login, "password": conn.password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=self._verify,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        self._token = resp.text.strip()
        self.log.info("NiFi: xac thuc thanh cong -> %s", self._base)

    def _headers(self) -> dict:
        self._connect()
        return {"Authorization": f"Bearer {self._token}"}

    def _set_state(self, pg_id: str, state: str) -> None:
        self._connect()
        resp = requests.put(
            f"{self._base}/nifi-api/flow/process-groups/{pg_id}",
            json={"id": pg_id, "state": state},
            headers={**self._headers(), "Content-Type": "application/json"},
            verify=self._verify,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        self.log.info("NiFi: process group %s -> %s", pg_id, state)

    def start_process_group(self, pg_id: str) -> None:
        self._set_state(pg_id, "RUNNING")

    def stop_process_group(self, pg_id: str) -> None:
        self._set_state(pg_id, "STOPPED")

    def queued_count(self, pg_id: str) -> int:
        """So flowfile con dang xep hang (recursive) trong process group."""
        self._connect()
        resp = requests.get(
            f"{self._base}/nifi-api/flow/process-groups/{pg_id}/status",
            params={"recursive": "true"},
            headers=self._headers(),
            verify=self._verify,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        snap = resp.json()["processGroupStatus"]["aggregateSnapshot"]
        return int(snap.get("flowFilesQueued", 0))
