"""Test pipeline-api bang FastAPI TestClient (AUTH_DISABLED de bo qua Keycloak).
Kiem tra registry tra ve code that cua job + config sinh catalog_env."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

os.environ["AUTH_DISABLED"] = "true"
os.environ["REGISTRY_PATH"] = os.path.join(HERE, "registry.yaml")
os.environ["WORKSPACE_ROOT"] = REPO_ROOT
os.environ["MINIO_ROOT_USER"] = "minio_access_key"
os.environ["MINIO_ROOT_PASSWORD"] = "minio_secret_key"

sys.path.insert(0, HERE)

from fastapi.testclient import TestClient  # noqa: E402

import app as app_module  # noqa: E402

client = TestClient(app_module.app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "UP"


def test_pipeline_metadata():
    r = client.get("/pipelines/bronze_xml")
    assert r.status_code == 200
    body = r.json()
    assert body["latest"] == "1.0.0"
    assert "1.0.0" in body["versions"]


def test_code_returns_real_job_source():
    r = client.get("/pipelines/bronze_xml/versions/latest/code")
    assert r.status_code == 200
    # Day dung la source job2 cua Kien
    assert "process_transactional_data" in r.text


def test_config_generates_catalog_env():
    r = client.get("/pipelines/bronze_master/versions/latest/config")
    assert r.status_code == 200
    cfg = r.json()
    assert cfg["version"] == "1.0.0"
    assert cfg["catalog_env"]["ICEBERG_CATALOG_URI"] == "thrift://hive-metastore:9083"
    assert cfg["catalog_env"]["AWS_ACCESS_KEY_ID"] == "minio_access_key"
    assert any("postgresql" in p for p in cfg["packages"])


def test_unknown_pipeline_404():
    assert client.get("/pipelines/khong_ton_tai").status_code == 404
