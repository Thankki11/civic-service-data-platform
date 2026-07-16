"""
Bootstrap flow NiFi "api_ingestion" qua REST API (thay cho import tay tren UI).

Flow: InvokeHTTP (GET http://mock-api:5000/api/payments/recent)
      -> PutS3Object (ghi landing-zone/api_payments_${now}.json tren MinIO)

Chay MOT LAN sau khi NiFi khoe:
    python platform/nifi/bootstrap_flow.py

Ket qua: in ra PROCESS_GROUP_ID. Dat vao Airflow Variable `nifi_api_pg_id`
de NifiOperator trigger:
    airflow variables set nifi_api_pg_id <id>

Bien moi truong (mac dinh doc tu .env qua compose):
    NIFI_BASE_URL   mac dinh https://localhost:8443
    NIFI_USER / NIFI_PASSWORD
    MINIO_ENDPOINT  mac dinh http://minio:9000
    MINIO_ACCESS_KEY / MINIO_SECRET_KEY  mac dinh minio_access_key/secret
"""
from __future__ import annotations

import os
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = os.getenv("NIFI_BASE_URL", "https://localhost:8443").rstrip("/")
USER = os.getenv("NIFI_USER", "admin")
PASSWORD = os.getenv("NIFI_PASSWORD", "change_me_nifi_12chars")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
API_URL = os.getenv("MOCK_API_URL", "http://mock-api:5000/api/payments/recent")

_S = requests.Session()
_S.verify = False


def _token() -> None:
    r = _S.post(
        f"{BASE}/nifi-api/access/token",
        data={"username": USER, "password": PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    _S.headers.update({"Authorization": f"Bearer {r.text.strip()}"})


def _root_pg_id() -> str:
    r = _S.get(f"{BASE}/nifi-api/flow/process-groups/root", timeout=30)
    r.raise_for_status()
    return r.json()["processGroupFlow"]["id"]


def _create_pg(parent: str, name: str) -> str:
    body = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": 0.0, "y": 0.0}},
    }
    r = _S.post(
        f"{BASE}/nifi-api/process-groups/{parent}/process-groups",
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]


def _add_processor(
    pg: str, ptype: str, name: str, props: dict, pos: tuple,
    scheduling_period: str | None = None,
) -> dict:
    config: dict = {"properties": props}
    if scheduling_period:
        # Chay THUA (vd 1 lan/gio) de "fetch mot lan" -> hang doi drain ve 0,
        # NifiOperator phat hien xong. Neu de mac dinh "0 sec" processor chay
        # lien tuc, hang doi khong bao gio rong -> operator treo toi timeout.
        config["schedulingStrategy"] = "TIMER_DRIVEN"
        config["schedulingPeriod"] = scheduling_period
    body = {
        "revision": {"version": 0},
        "component": {
            "type": ptype,
            "name": name,
            "position": {"x": pos[0], "y": pos[1]},
            "config": config,
        },
    }
    r = _S.post(
        f"{BASE}/nifi-api/process-groups/{pg}/processors", json=body, timeout=30
    )
    r.raise_for_status()
    return r.json()


def _autoterminate(proc: dict, relationships: list[str]) -> None:
    proc_id = proc["id"]
    body = {
        "revision": proc["revision"],
        "component": {
            "id": proc_id,
            "config": {"autoTerminatedRelationships": relationships},
        },
    }
    r = _S.put(f"{BASE}/nifi-api/processors/{proc_id}", json=body, timeout=30)
    r.raise_for_status()


def _connect(pg: str, src: dict, dst: dict, rels: list[str]) -> None:
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src["id"], "groupId": pg, "type": "PROCESSOR"},
            "destination": {"id": dst["id"], "groupId": pg, "type": "PROCESSOR"},
            "selectedRelationships": rels,
        },
    }
    r = _S.post(
        f"{BASE}/nifi-api/process-groups/{pg}/connections", json=body, timeout=30
    )
    r.raise_for_status()


def main() -> None:
    _token()
    root = _root_pg_id()
    pg = _create_pg(root, "api_ingestion")

    invoke = _add_processor(
        pg,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "InvokeHTTP_payments",
        {"HTTP Method": "GET", "Remote URL": API_URL},
        (0.0, 0.0),
        scheduling_period="3600 sec",
    )
    # Cac quan he khong dung -> tu ket thuc, tranh flow ket noi treo
    _autoterminate(
        invoke,
        ["Original", "Retry", "No Retry", "Failure"],
    )

    put_s3 = _add_processor(
        pg,
        "org.apache.nifi.processors.aws.s3.PutS3Object",
        "PutS3_landing",
        {
            "Bucket": "landing-zone",
            "Object Key": "api_payments_${now():format('yyyyMMddHHmmssSSS')}.json",
            "Access Key": MINIO_ACCESS_KEY,
            "Secret Key": MINIO_SECRET_KEY,
            "Endpoint Override URL": MINIO_ENDPOINT,
            "Region": "us-east-1",
        },
        (0.0, 400.0),
    )
    _autoterminate(put_s3, ["success", "failure"])

    _connect(pg, invoke, put_s3, ["Response"])

    print(pg)  # STDOUT chi in PG id de tien capture -> Airflow Variable


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(f"Loi goi NiFi API: {exc} -> {exc.response.text}", file=sys.stderr)
        sys.exit(1)
