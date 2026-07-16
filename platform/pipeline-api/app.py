"""
Pipeline-version API (Layer 6 control-plane).

Phuc vu MA PySpark + CONFIG theo PIPELINE va VERSION, bao ve bang Keycloak
(JWT RS256). CustomSparkSubmitOperator trong Airflow goi service nay de lay
code job cua Kien/Quan (thay vi doc file cung), tu do sinh ENV catalog cho
spark-submit.

Endpoints:
  GET /health                                  -> khong can auth
  GET /pipelines/{name}                         -> metadata + latest version
  GET /pipelines/{name}/versions/{ver}/code     -> source PySpark (text/plain)
  GET /pipelines/{name}/versions/{ver}/config   -> spark_conf + packages + catalog_env

Bien moi truong:
  KEYCLOAK_ISSUER   vd http://keycloak:8080/realms/lakehouse
  REGISTRY_PATH     mac dinh /app/registry.yaml
  WORKSPACE_ROOT    goc repo mount read-only, mac dinh /workspace
  MINIO_ROOT_USER / MINIO_ROOT_PASSWORD  -> bom vao catalog_env (AWS creds)
  AUTH_DISABLED     "true" de tat auth (chi dung khi test cuc bo)
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt
import yaml
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

KEYCLOAK_ISSUER = os.getenv("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/lakehouse")
REGISTRY_PATH = Path(os.getenv("REGISTRY_PATH", "/app/registry.yaml"))
WORKSPACE_ROOT = Path(os.getenv("WORKSPACE_ROOT", "/workspace")).resolve()
AUTH_DISABLED = os.getenv("AUTH_DISABLED", "false").lower() == "true"

app = FastAPI(title="Pipeline-version API", version="1.0.0")
_bearer = HTTPBearer(auto_error=False)


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def _registry() -> dict[str, Any]:
    with REGISTRY_PATH.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("pipelines", {})


def _get_pipeline(name: str) -> dict[str, Any]:
    pipe = _registry().get(name)
    if pipe is None:
        raise HTTPException(status_code=404, detail=f"Pipeline khong ton tai: {name}")
    return pipe


def _resolve_version(pipe: dict[str, Any], version: str) -> tuple[str, dict[str, Any]]:
    if version in ("latest", "", None):
        version = pipe.get("latest")
    ver = pipe.get("versions", {}).get(version)
    if ver is None:
        raise HTTPException(status_code=404, detail=f"Version khong ton tai: {version}")
    return version, ver


# --------------------------------------------------------------------------- #
# Auth (Keycloak JWT RS256)
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def _jwks_client() -> jwt.PyJWKClient:
    return jwt.PyJWKClient(f"{KEYCLOAK_ISSUER}/protocol/openid-connect/certs")


def verify_token(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> dict[str, Any]:
    if AUTH_DISABLED:
        return {"sub": "auth-disabled"}
    if creds is None or not creds.credentials:
        raise HTTPException(status_code=401, detail="Thieu Bearer token")
    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(creds.credentials)
        return jwt.decode(
            creds.credentials,
            signing_key.key,
            algorithms=["RS256"],
            issuer=KEYCLOAK_ISSUER,
            options={"verify_aud": False},
        )
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail=f"Token khong hop le: {exc}") from exc


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _read_code(code_path: str) -> str:
    # Chong path traversal: bat buoc nam trong WORKSPACE_ROOT
    target = (WORKSPACE_ROOT / code_path).resolve()
    if not str(target).startswith(str(WORKSPACE_ROOT)):
        raise HTTPException(status_code=400, detail="code_path khong hop le")
    if not target.is_file():
        raise HTTPException(status_code=404, detail=f"Khong tim thay code: {code_path}")
    return target.read_text(encoding="utf-8")


def _catalog_env(spark_conf: dict[str, Any]) -> dict[str, str]:
    """Tu sinh ENV catalog tu spark_conf (secret bom tu env cua service)."""
    env: dict[str, str] = {}
    if "spark.sql.catalog.lakehouse.uri" in spark_conf:
        env["ICEBERG_CATALOG_URI"] = str(spark_conf["spark.sql.catalog.lakehouse.uri"])
    if "spark.sql.catalog.lakehouse.warehouse" in spark_conf:
        env["ICEBERG_WAREHOUSE"] = str(spark_conf["spark.sql.catalog.lakehouse.warehouse"])
    if "spark.hadoop.fs.s3a.endpoint" in spark_conf:
        env["S3_ENDPOINT"] = str(spark_conf["spark.hadoop.fs.s3a.endpoint"])
    access = os.getenv("MINIO_ROOT_USER")
    secret = os.getenv("MINIO_ROOT_PASSWORD")
    if access and secret:
        env["AWS_ACCESS_KEY_ID"] = access
        env["AWS_SECRET_ACCESS_KEY"] = secret
    return env


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "UP", "pipelines": str(len(_registry()))}


@app.get("/pipelines/{name}")
def get_pipeline(name: str, _claims: dict = Depends(verify_token)) -> dict[str, Any]:
    pipe = _get_pipeline(name)
    return {
        "name": name,
        "description": pipe.get("description"),
        "latest": pipe.get("latest"),
        "versions": sorted(pipe.get("versions", {}).keys()),
    }


@app.get("/pipelines/{name}/versions/{version}/code", response_class=PlainTextResponse)
def get_code(name: str, version: str, _claims: dict = Depends(verify_token)) -> str:
    _, ver = _resolve_version(_get_pipeline(name), version)
    return _read_code(ver["code_path"])


@app.get("/pipelines/{name}/versions/{version}/config")
def get_config(name: str, version: str, _claims: dict = Depends(verify_token)) -> dict[str, Any]:
    resolved, ver = _resolve_version(_get_pipeline(name), version)
    spark_conf = ver.get("spark_conf", {})
    return {
        "pipeline": name,
        "version": resolved,
        "spark_conf": spark_conf,
        "packages": ver.get("packages", []),
        "catalog_env": _catalog_env(spark_conf),
    }
