"""Test CustomSparkSubmitOperator: xac thuc Keycloak + lay code/config tu
pipeline-api (mock), kiem tra application/conf/packages/env_vars duoc set dung
truoc khi goi spark-submit."""
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import operators.custom_spark_submit_operator as mod
from operators.custom_spark_submit_operator import CustomSparkSubmitOperator

CODE = "print('hello from pipeline-api')\n"
CONFIG = {
    "pipeline": "bronze_xml",
    "version": "1.0.0",
    "spark_conf": {"spark.sql.catalog.lakehouse.type": "hive"},
    "packages": [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ],
    "catalog_env": {"AWS_ACCESS_KEY_ID": "minio_access_key"},
}


class _Resp:
    def __init__(self, text="", payload=None):
        self.text = text
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _fake_get(url, headers=None, timeout=None):
    assert headers["Authorization"] == "Bearer TESTTOKEN"
    if url.endswith("/code"):
        return _Resp(text=CODE)
    if url.endswith("/config"):
        return _Resp(payload=CONFIG)
    raise AssertionError(f"URL bat ngo: {url}")


class _FakeKeycloak:
    def __init__(self, conn_id):
        pass

    def get_token(self, force=False):
        return "TESTTOKEN"


def test_execute_fetches_and_sets(monkeypatch):
    captured = {}

    def _fake_super_execute(self, context):
        captured["application"] = self.application
        captured["conf"] = self.conf
        captured["packages"] = self.packages
        captured["env_vars"] = self.env_vars
        return "done"

    monkeypatch.setattr(mod, "KeycloakHook", _FakeKeycloak)
    monkeypatch.setattr(mod.requests, "get", _fake_get)
    monkeypatch.setattr(SparkSubmitOperator, "execute", _fake_super_execute)

    op = CustomSparkSubmitOperator(
        task_id="spark_x",
        pipeline_name="bronze_xml",
        pipeline_version="1.0.0",
        pipeline_api_base="http://pipeline-api:8000",
    )
    result = op.execute(context={})

    assert result == "done"
    # application tro toi file tam chua dung code lay tu API
    assert captured["application"].endswith("_bronze_xml.py")
    with open(captured["application"], encoding="utf-8") as fh:
        assert fh.read() == CODE
    # conf gop spark_conf tu API
    assert captured["conf"]["spark.sql.catalog.lakehouse.type"] == "hive"
    # packages la chuoi phan cach dau phay
    assert captured["packages"] == ",".join(CONFIG["packages"])
    # catalog_env duoc bom vao env_vars
    assert captured["env_vars"]["AWS_ACCESS_KEY_ID"] == "minio_access_key"
