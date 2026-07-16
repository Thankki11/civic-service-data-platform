"""
CustomSparkSubmitOperator — mo rong SparkSubmitOperator chuan de:

  1. Xac thuc Keycloak (ROPC) qua KeycloakHook -> access_token.
  2. Lay MA PySpark tu pipeline-version API theo (pipeline_name, version)
     thay vi tro toi 1 file .py cung.
  3. Tu sinh bien moi truong catalog (S3/Iceberg) tu spark_conf ma API tra ve,
     dong thoi map spark_conf -> --conf va packages -> --packages.
  4. Goi super().execute() de spark-submit toi spark_default.

Vi du:
    CustomSparkSubmitOperator(
        task_id="spark_job2_xml",
        pipeline_name="bronze_xml",
        pipeline_version="latest",
        name="ingestion-{{ ds }}",
    )
"""
from __future__ import annotations

import tempfile

import requests
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from hooks.keycloak_hook import KeycloakHook

_TIMEOUT = 30


class CustomSparkSubmitOperator(SparkSubmitOperator):
    template_fields = tuple(SparkSubmitOperator.template_fields) + (
        "pipeline_name",
        "pipeline_version",
    )

    def __init__(
        self,
        pipeline_name: str,
        pipeline_version: str = "latest",
        keycloak_conn_id: str = "keycloak_default",
        pipeline_api_base: str | None = None,
        conn_id: str = "spark_default",
        **kwargs,
    ) -> None:
        # application se duoc set trong execute() sau khi tai code tu API
        super().__init__(application="", conn_id=conn_id, **kwargs)
        self.pipeline_name = pipeline_name
        self.pipeline_version = pipeline_version
        self.keycloak_conn_id = keycloak_conn_id
        self.pipeline_api_base = pipeline_api_base
        self._tmp_code: str | None = None

    def _api_base(self) -> str:
        if self.pipeline_api_base:
            return self.pipeline_api_base.rstrip("/")
        return Variable.get(
            "pipeline_api_base", default_var="http://pipeline-api:8000"
        ).rstrip("/")

    def _set_attr(self, name: str, value) -> None:
        """Set ca thuoc tinh public va _underscore (chong lech version provider)."""
        if hasattr(self, name):
            setattr(self, name, value)
        under = f"_{name}"
        if hasattr(self, under):
            setattr(self, under, value)

    def execute(self, context):
        token = KeycloakHook(self.keycloak_conn_id).get_token()
        headers = {"Authorization": f"Bearer {token}"}
        base = self._api_base()
        name, ver = self.pipeline_name, self.pipeline_version
        self.log.info("pipeline-api: lay code/config cho %s@%s tu %s", name, ver, base)

        code = requests.get(
            f"{base}/pipelines/{name}/versions/{ver}/code",
            headers=headers,
            timeout=_TIMEOUT,
        )
        code.raise_for_status()

        cfg = requests.get(
            f"{base}/pipelines/{name}/versions/{ver}/config",
            headers=headers,
            timeout=_TIMEOUT,
        )
        cfg.raise_for_status()
        cfg = cfg.json()

        # 1) Ghi code ra file tam roi tro application vao do
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_{name}.py", delete=False, encoding="utf-8"
        )
        tmp.write(code.text)
        tmp.close()
        self._tmp_code = tmp.name
        self._set_attr("application", tmp.name)

        # 2) spark_conf -> --conf (gop voi conf san co neu co)
        merged_conf = dict(self.conf or {})
        merged_conf.update(cfg.get("spark_conf", {}))
        self._set_attr("conf", merged_conf)

        # 3) packages -> --packages (chuoi phan cach dau phay)
        packages = cfg.get("packages", [])
        if packages:
            self._set_attr("packages", ",".join(packages))

        # 4) catalog_env -> bien moi truong cho tien trinh spark-submit
        merged_env = dict(self.env_vars or {})
        merged_env.update(cfg.get("catalog_env", {}))
        if merged_env:
            self._set_attr("env_vars", merged_env)

        self.log.info(
            "CustomSparkSubmit: chay %s@%s (%d conf, %d packages)",
            name,
            cfg.get("version", ver),
            len(merged_conf),
            len(packages),
        )
        return super().execute(context)

    def on_kill(self) -> None:
        super().on_kill()
