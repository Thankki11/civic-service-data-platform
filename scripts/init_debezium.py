"""Idempotently register/update the PostgreSQL Debezium connector."""

import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://debezium-connect:8083")
CONFIG_PATH = Path(
    os.getenv("DEBEZIUM_CONFIG_PATH", "/opt/project/ingestion/debezium_config.json")
)
TIMEOUT_SECONDS = int(os.getenv("INIT_TIMEOUT_SECONDS", "180"))


def request_json(method: str, path: str, payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
        return response.status, json.loads(body) if body else None


def wait_for_connect(deadline: float) -> None:
    while time.time() < deadline:
        try:
            request_json("GET", "/connectors")
            return
        except (HTTPError, URLError, TimeoutError):
            time.sleep(2)
    raise TimeoutError("Debezium Connect did not become ready")


def main() -> None:
    document = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    name = document["name"]
    config = document["config"]
    deadline = time.time() + TIMEOUT_SECONDS
    wait_for_connect(deadline)

    try:
        request_json("GET", f"/connectors/{name}")
        request_json("PUT", f"/connectors/{name}/config", config)
        print(f"[+] Updated Debezium connector: {name}")
    except HTTPError as exc:
        if exc.code != 404:
            raise
        request_json("POST", "/connectors", document)
        print(f"[+] Created Debezium connector: {name}")

    while time.time() < deadline:
        try:
            _, status = request_json("GET", f"/connectors/{name}/status")
            connector_running = status["connector"]["state"] == "RUNNING"
            tasks = status.get("tasks", [])
            tasks_running = bool(tasks) and all(task["state"] == "RUNNING" for task in tasks)
            if connector_running and tasks_running:
                print(f"[+] Debezium connector is RUNNING: {name}")
                return
            failed = [task for task in tasks if task["state"] == "FAILED"]
            if failed:
                raise RuntimeError(f"Debezium task failed: {failed}")
        except (HTTPError, URLError, TimeoutError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Debezium connector did not reach RUNNING state: {name}")


if __name__ == "__main__":
    main()
