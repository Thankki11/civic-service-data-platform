"""
NifiOperator — trigger 1 process group NiFi va (tuy chon) cho toi khi hang
doi rong het (flow xu ly xong) roi dung lai.

Vi du:
    NifiOperator(
        task_id="nifi_fetch_api",
        process_group_name="api_ingestion",
        wait_for_completion=True,
    )
"""
from __future__ import annotations

import time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from hooks.nifi_hook import NifiHook


class NifiOperator(BaseOperator):
    template_fields = ("process_group_id", "process_group_name")

    def __init__(
        self,
        process_group_id: str | None = None,
        process_group_name: str | None = None,
        conn_id: str = "nifi_default",
        wait_for_completion: bool = True,
        poll_interval: int = 10,
        timeout: int = 900,
        stable_polls: int = 3,
        stop_after: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not process_group_id and not process_group_name:
            raise ValueError("Can process_group_id hoac process_group_name")
        self.process_group_id = process_group_id
        self.process_group_name = process_group_name
        self.conn_id = conn_id
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.stable_polls = stable_polls
        self.stop_after = stop_after

    def execute(self, context) -> None:
        hook = NifiHook(self.conn_id)
        pg_id = self.process_group_id
        if not pg_id:
            pg_id = hook.find_process_group_id(self.process_group_name)
            self.log.info(
                "NiFi: da tim process group %s -> %s",
                self.process_group_name,
                pg_id,
            )

        self.log.info("NiFi: khoi dong process group %s", pg_id)
        hook.start_process_group(pg_id)

        if not self.wait_for_completion:
            return

        deadline = time.time() + self.timeout
        empty_streak = 0
        while time.time() < deadline:
            time.sleep(self.poll_interval)
            queued = hook.queued_count(pg_id)
            self.log.info("NiFi: con %s flowfile trong hang doi", queued)
            if queued == 0:
                empty_streak += 1
                if empty_streak >= self.stable_polls:
                    self.log.info("NiFi: hang doi da rong on dinh -> hoan tat")
                    if self.stop_after:
                        hook.stop_process_group(pg_id)
                    return
            else:
                empty_streak = 0

        if self.stop_after:
            hook.stop_process_group(pg_id)
        raise AirflowException(
            f"NiFi process group {pg_id} khong drain xong "
            f"sau {self.timeout}s"
        )
