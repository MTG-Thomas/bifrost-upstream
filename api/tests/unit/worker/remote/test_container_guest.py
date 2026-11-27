"""Container guest identity, readiness and process retirement contracts."""

import json
import signal
from multiprocessing import Pipe
from pathlib import Path
from unittest.mock import Mock

import pytest
from src.worker.remote import container_guest
from src.worker.remote.guest_diagnostics import guest_diagnostic_payload, guest_diagnostic_signature, verify_guest_diagnostic
from src.worker.remote.guest_init import CAP_MASKS, FACTS, INIT_CODES, GuestInitError, collect_guest_facts
from src.worker.remote.protocol import AuthenticationError


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
    send_ready.send_bytes(FACTS)
    send_ready.send_bytes(json.dumps(collect_guest_facts(supervisor=False)).encode())
    send_ready.send_bytes(container_guest.READY)
    guest.wait_ready()
    assert guest.take() == (guest.process.pid, guest.work, guest.result)
    with pytest.raises(MemoryError, match="unavailable"):
        guest.take()


@pytest.mark.parametrize(("returncode", "reason"), [(0, "ready-eof"), (1, "ready-exit"), (-9, "ready-signal")])
def test_ready_pipe_eof_retains_child_exit_evidence(guest_pipes, returncode, reason) -> None:
    guest, send_ready = guest_pipes
    guest.process.wait.return_value = returncode
    send_ready.close()
    with pytest.raises(GuestInitError) as failure:
        guest.wait_ready()
    assert failure.value.reason == reason
    assert failure.value.returncode == returncode


@pytest.mark.parametrize(("frame", "reason"), [(b"too long", "ready-length"), (b"", "ready-length"), (b"!", "ready-unknown")])
def test_invalid_ready_frame_is_classified(guest_pipes, frame, reason) -> None:
    guest, send_ready = guest_pipes
    send_ready.send_bytes(frame)
    with pytest.raises(GuestInitError) as failure:
        guest.wait_ready()
    assert failure.value.reason == reason


def test_ready_timeout_is_distinct_from_child_exit(guest_pipes, monkeypatch) -> None:
    guest, _ = guest_pipes
    monkeypatch.setattr(container_guest, "TEMPLATE_START_TIMEOUT_SECONDS", 0)
    with pytest.raises(GuestInitError) as failure:
        guest.wait_ready()
    assert failure.value.reason == "ready-timeout" and failure.value.returncode is None


@pytest.mark.parametrize("phase", INIT_CODES.values())
def test_child_errors_emit_only_a_one_byte_class(guest_pipes, monkeypatch, phase) -> None:
    guest, send_ready = guest_pipes
    facts = collect_guest_facts(supervisor=False)
    facts.update(ca_readable=phase != "ca-unreadable", scratch_writable=phase != "scratch-unwritable",
                 shm_exists=phase != "shm-unavailable")
    monkeypatch.setattr(container_guest, "collect_guest_facts", lambda **_: facts)
    monkeypatch.setattr(container_guest.os, "getresuid", lambda: (1001,) * 3 if phase != "identity" else (1000,) * 3)
    monkeypatch.setattr(container_guest.os, "getresgid", lambda: (1001,) * 3)
    monkeypatch.setattr(container_guest.os, "getgroups", lambda: [])
    fault = OSError("secret-only-in-guest")
    monkeypatch.setattr(container_guest, "_prctl", Mock(side_effect=fault if phase == "prctl" else None))
    monkeypatch.setattr(container_guest, "proc_status", lambda: {
        name: "1" if phase == "capabilities" else "0" for name in CAP_MASKS
    })
    monkeypatch.setattr(container_guest.multiprocessing, "set_start_method", Mock(
        side_effect=fault if phase in {"shm-unavailable", "other"} else None,
    ))

    def load_runtime(pipe, *, note_phase):
        assert pipe is None
        note_phase(phase)
        raise fault

    monkeypatch.setattr(container_guest, "_load_workflow_runtime", load_runtime)
    assert not container_guest._initialize_guest(send_ready)
    guest.process.wait.return_value = 1
    with pytest.raises(GuestInitError) as failure:
        guest.wait_ready()
    assert failure.value.reason == phase and failure.value.returncode == 1
    payload = guest_diagnostic_payload("guest-init", failure.value, startup_facts=guest.startup_facts)
    assert "secret-only-in-guest" not in json.dumps(payload)
    signature = guest_diagnostic_signature("key", 1, 1, payload)
    assert verify_guest_diagnostic("key", 1, 1, {**payload, "signature": signature}) == payload


def test_signed_guest_facts_reject_arbitrary_fields_and_values() -> None:
    facts = collect_guest_facts(supervisor=True)
    for extra in ({"secret": "hidden"}, {"kernel": "6.8.0-hostname"}, {"cpu_count": True}, {"CapBnd": "not hex"}):
        body = guest_diagnostic_payload("guest-init", startup_facts={"supervisor": {**facts, **extra}})
        with pytest.raises(AuthenticationError):
            verify_guest_diagnostic("key", 1, 1, {
                **body, "signature": guest_diagnostic_signature("key", 1, 1, body),
            })


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
