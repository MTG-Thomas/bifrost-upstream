"""Container guest identity, readiness and process retirement contracts."""

import signal
from multiprocessing import Pipe
from pathlib import Path
from unittest.mock import Mock

import pytest
from src.worker.remote import container_guest


@pytest.fixture
def guest_pipes():
    ready, send_ready = Pipe(duplex=False)
    read_work, work = Pipe(duplex=False)
    result, send_result = Pipe(duplex=False)
    process = Mock(pid=7)
    process.poll.return_value = None
    guest = container_guest.ContainerGuest(process, ready, work, result)
    try:
        yield guest, send_ready
    finally:
        guest.close()
        for pipe in (send_ready, read_work, send_result):
            pipe.close()


def test_guest_can_be_taken_once_after_readiness(guest_pipes) -> None:
    guest, send_ready = guest_pipes
    send_ready.send_bytes(container_guest.READY)
    guest.wait_ready()
    assert guest.take() == (guest.process.pid, guest.work, guest.result)
    with pytest.raises(MemoryError, match="unavailable"):
        guest.take()


def test_an_exited_guest_is_never_taken(guest_pipes) -> None:
    guest, _ = guest_pipes
    guest.process.poll.return_value = 1
    assert guest.exited
    with pytest.raises(MemoryError, match="unavailable"):
        guest.take()
    assert not guest.used


def test_guest_pidfd_signal_targets_only_the_dedicated_uid(tmp_path, monkeypatch, guest_pipes) -> None:
    guest, _ = guest_pipes
    for pid, uid in [(100, 1000), (101, 1001)]:
        process = tmp_path / str(pid)
        process.mkdir()
        (process / "status").write_text(f"Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n")
    with monkeypatch.context() as patch:
        patch.setattr(container_guest, "Path", lambda _: tmp_path)
        patch.setattr(container_guest.os, "pidfd_open", lambda pid: pid + 10)
        send = Mock()
        close = Mock()
        patch.setattr(container_guest.signal, "pidfd_send_signal", send)
        patch.setattr(container_guest.os, "close", close)
        guest._signal_guest(signal.SIGKILL)
    send.assert_called_once_with(111, signal.SIGKILL)
    assert {call.args[0] for call in close.call_args_list} == {110, 111}


@pytest.mark.asyncio
async def test_guest_stop_waits_for_reaping_after_a_forced_signal(guest_pipes, monkeypatch) -> None:
    guest, _ = guest_pipes
    monkeypatch.setattr(guest, "_reap_guest", Mock(side_effect=[False, True]))
    sent = Mock()
    monkeypatch.setattr(guest, "_signal_guest", sent)
    assert await guest.stop()
    sent.assert_called_once_with(signal.SIGKILL)


@pytest.mark.asyncio
async def test_guest_stop_reports_unconfirmed_process_evidence(guest_pipes, monkeypatch) -> None:
    guest, _ = guest_pipes
    monkeypatch.setattr(guest, "_signal_guest", Mock(side_effect=PermissionError("process evidence unavailable")))
    assert not await guest.stop()


def test_supervisor_refuses_capability_changes_with_multiple_threads(monkeypatch) -> None:
    jobs = Mock()
    jobs.iterdir.return_value = iter([Path("1"), Path("2")])
    prctl = Mock()
    monkeypatch.setattr(container_guest, "Path", lambda _: jobs)
    monkeypatch.setattr(container_guest, "_prctl", prctl)
    with pytest.raises(RuntimeError, match="single-threaded"):
        container_guest._drop_supervisor_privileges()
    prctl.assert_not_called()
