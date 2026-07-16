"""Test NifiOperator voi NifiHook gia lap (khong goi NiFi that)."""
import pytest
from airflow.exceptions import AirflowException

import operators.nifi_operator as nifi_operator_mod
from operators.nifi_operator import NifiOperator


class _FakeHook:
    """Hook gia: tra ve chuoi so luong flowfile theo kich ban."""

    def __init__(self, queue_sequence):
        self._seq = list(queue_sequence)
        self.started = False
        self.stopped = False

    def start_process_group(self, pg_id):
        self.started = True

    def stop_process_group(self, pg_id):
        self.stopped = True

    def queued_count(self, pg_id):
        return self._seq.pop(0) if self._seq else 0


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(nifi_operator_mod.time, "sleep", lambda *_: None)


def test_drains_and_stops(monkeypatch):
    fake = _FakeHook([5, 2, 0, 0, 0])  # giam dan roi rong on dinh
    monkeypatch.setattr(nifi_operator_mod, "NifiHook", lambda conn_id: fake)

    op = NifiOperator(
        task_id="t", process_group_id="pg-1", poll_interval=0, stable_polls=3
    )
    op.execute(context={})

    assert fake.started and fake.stopped


def test_timeout_raises(monkeypatch):
    fake = _FakeHook([9] * 100)  # khong bao gio rong
    monkeypatch.setattr(nifi_operator_mod, "NifiHook", lambda conn_id: fake)

    op = NifiOperator(
        task_id="t", process_group_id="pg-1", poll_interval=0, timeout=0
    )
    with pytest.raises(AirflowException):
        op.execute(context={})


def test_no_wait_returns_immediately(monkeypatch):
    fake = _FakeHook([])
    monkeypatch.setattr(nifi_operator_mod, "NifiHook", lambda conn_id: fake)

    op = NifiOperator(
        task_id="t", process_group_id="pg-1", wait_for_completion=False
    )
    op.execute(context={})

    assert fake.started and not fake.stopped
