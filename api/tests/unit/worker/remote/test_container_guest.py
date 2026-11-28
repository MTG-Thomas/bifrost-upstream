"""Container guest identity, readiness and process retirement contracts."""

import ctypes
import errno
import json
import os
import signal
from multiprocessing import Pipe
from pathlib import Path
from unittest.mock import Mock

import pytest
from src.worker.remote import container_guest
from src.worker.remote.guest_diagnostics import guest_diagnostic_payload, guest_diagnostic_signature, verify_guest_diagnostic
from src.worker.remote.guest_init import CAP_MASKS, FACTS, INIT_CODES, GuestInitError, SupervisorPrivilegeError, collect_guest_facts
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


@pytest.mark.parametrize("fragment", [b"\x00", b"\x00\x00\x00\x01"])
def test_partial_ready_frame_cannot_block_past_deadline(guest_pipes, monkeypatch, fragment) -> None:
    guest, send_ready = guest_pipes
    monkeypatch.setattr(container_guest, "TEMPLATE_START_TIMEOUT_SECONDS", 0.05)
    os.write(send_ready.fileno(), fragment)
    with pytest.raises(GuestInitError, match="initialization failed") as failure:
        guest.wait_ready()
    assert failure.value.reason == "ready-timeout"


def test_guest_readiness_wire_bytes_are_stable() -> None:
    assert (container_guest.READY, FACTS) == (b"\x01", b"\x02")
    assert INIT_CODES == {
        b"\x10": "identity", b"\x11": "prctl", b"\x12": "capabilities", b"\x13": "ca-unreadable",
        b"\x15": "scratch-unwritable", b"\x16": "runtime-import", b"\x17": "settings", b"\x18": "hook",
        b"\x19": "other",
    }


@pytest.mark.parametrize("phase", INIT_CODES.values())
def test_child_errors_emit_only_a_one_byte_class(guest_pipes, monkeypatch, phase) -> None:
    guest, send_ready = guest_pipes
    facts = collect_guest_facts(supervisor=False)
    facts.update(ca_readable=phase != "ca-unreadable", scratch_writable=phase != "scratch-unwritable")
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
        side_effect=fault if phase == "other" else None,
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
    with pytest.raises(SupervisorPrivilegeError, match="single-thread"):
        container_guest._drop_supervisor_privileges()
    prctl.assert_not_called()


def test_prepare_guest_capabilities_clears_both_inheritable_words(monkeypatch) -> None:
    guest = container_guest
    kernel = Mock()
    kernel.capset.return_value = kernel.prctl.return_value = 0

    def capget(_header, masks):
        words = ctypes.cast(masks, ctypes.POINTER(guest._CapabilitySet * 2)).contents
        words[0] = guest._CapabilitySet(0x1E0, 0x1F0, 0x20)
        words[1] = guest._CapabilitySet(0x10, 0x30, 0x10)
        return 0

    written = []

    def capset(_header, masks):
        words = ctypes.cast(masks, ctypes.POINTER(guest._CapabilitySet * 2)).contents
        written.extend((mask.effective, mask.permitted, mask.inheritable) for mask in words)
        return 0

    kernel.capget.side_effect = capget
    kernel.capset.side_effect = capset
    monkeypatch.setattr(guest.ctypes, "CDLL", Mock(return_value=kernel))
    monkeypatch.setattr(guest, "_require_single_threaded_init", Mock())
    monkeypatch.setattr(guest, "proc_status", lambda: {"CapBnd": "0000001000000020"})

    guest._prepare_guest_capabilities()

    assert written == [(0x1E0, 0x1F0, 0), (0x10, 0x30, 0)]
    assert [call.args for call in kernel.prctl.call_args_list] == [
        (24, 5, 0, 0, 0), (24, 36, 0, 0, 0), (38, 1, 0, 0, 0),
    ]
    assert [call[0] for call in kernel.mock_calls] == ["capget", "capset", "prctl", "prctl", "prctl"]


@pytest.mark.parametrize(("failure", "step", "bit"), [
    ("capget", "capget", None),
    ("capset", "inheritable-clear", None),
    ((24, 5), "bounding-drop", 5),
    ((24, 36), "bounding-drop", 36),
    ((38, 1), "no-new-privs", None),
])
def test_prepare_guest_capabilities_fails_closed_on_kernel_error(monkeypatch, failure, step, bit) -> None:
    guest = container_guest
    kernel = Mock()
    kernel.capget.return_value = kernel.capset.return_value = kernel.prctl.return_value = 0
    if isinstance(failure, str):
        getattr(kernel, failure).return_value = -1
    else:
        kernel.prctl.side_effect = lambda option, value, *_: -1 if (option, value) == failure else 0
    monkeypatch.setattr(guest.ctypes, "CDLL", Mock(return_value=kernel))
    monkeypatch.setattr(guest.ctypes, "get_errno", lambda: errno.EPERM)
    monkeypatch.setattr(guest, "_require_single_threaded_init", Mock())
    monkeypatch.setattr(guest, "proc_status", lambda: {"CapBnd": "0000001000000020"})
    monkeypatch.setattr(guest.os, "getuid", lambda: 0)
    monkeypatch.setattr(guest.os, "getpid", lambda: 1)
    spawn = Mock()
    monkeypatch.setattr(guest.subprocess, "Popen", spawn)

    with pytest.raises(SupervisorPrivilegeError) as failure_info:
        guest.ContainerGuest.start_image("https://ingress.example.test")
    exc = failure_info.value
    assert (exc.step, exc.errno_class, exc.bit) == (step, "EPERM", bit)
    spawn.assert_not_called()
    if bit == 36:
        assert [call.args[:2] for call in kernel.prctl.call_args_list] == [(24, 5), (24, 36)]


@pytest.mark.parametrize(("operation", "step"), [
    ((8, 1), "keepcaps-enable"),
    ("setgroups", "setgroups"),
    ("setresgid", "setresgid"),
    ("setresuid", "setresuid"),
    ("capset", "supervisor-capset"),
    ((8, 0), "keepcaps-disable"),
])
def test_supervisor_drop_classifies_each_step(monkeypatch, operation, step) -> None:
    guest = container_guest
    denied = OSError(errno.EPERM, "do-not-publish-this")

    def prctl(option, value):
        if (option, value) == operation:
            raise denied

    monkeypatch.setattr(guest, "_require_single_threaded_init", Mock())
    monkeypatch.setattr(guest, "_prctl", prctl)
    for syscall in ("setgroups", "setresgid", "setresuid"):
        monkeypatch.setattr(guest.os, syscall, Mock(side_effect=denied if syscall == operation else None))
    monkeypatch.setattr(guest, "_set_capability_masks", Mock(side_effect=denied if operation == "capset" else None))

    with pytest.raises(SupervisorPrivilegeError) as failure:
        guest._drop_supervisor_privileges()
    assert (failure.value.step, failure.value.errno_class) == (step, "EPERM")
    assert "do-not-publish-this" not in str(failure.value)


@pytest.mark.parametrize("failure", ["supervisor-identity", "bounding-read", "guest-spawn"])
def test_start_image_classifies_other_supervisor_setup_errors(monkeypatch, failure) -> None:
    guest = container_guest
    monkeypatch.setattr(guest.os, "getpid", lambda: 2 if failure == "supervisor-identity" else 1)
    monkeypatch.setattr(guest.os, "getuid", lambda: 0)
    monkeypatch.setattr(guest, "_require_single_threaded_init", Mock())
    kernel = Mock()
    kernel.capset.return_value = kernel.prctl.return_value = kernel.capget.return_value = 0
    monkeypatch.setattr(guest.ctypes, "CDLL", Mock(return_value=kernel))
    monkeypatch.setattr(guest, "proc_status", lambda: {} if failure == "bounding-read" else {"CapBnd": "0"})
    monkeypatch.setattr(guest.subprocess, "Popen", Mock(side_effect=OSError(errno.EACCES, "private path")))

    with pytest.raises(SupervisorPrivilegeError) as error:
        guest.ContainerGuest.start_image("https://ingress.example.test")
    assert error.value.step == failure
    assert error.value.errno_class == ("EACCES" if failure == "guest-spawn" else "none")


def test_guest_does_not_initialize_without_supervisor_start_grant(monkeypatch) -> None:
    read_work, write_work = Pipe(duplex=False)
    read_ready, write_ready = Pipe(duplex=False)
    read_result, write_result = Pipe(duplex=False)
    initialize = Mock()
    monkeypatch.setattr(container_guest, "_initialize_guest", initialize)
    with read_work, write_work, read_ready, write_ready, read_result, write_result:
        write_work.close()
        assert container_guest._guest_main(
            os.dup(write_ready.fileno()), os.dup(read_work.fileno()), os.dup(write_result.fileno()),
        ) == 1
    initialize.assert_not_called()


@pytest.mark.parametrize(("field", "bad_value"), [
    ("capability_bit", -1), ("capability_bit", 64), ("capability_bit", True),
    ("errno_class", "injected untrusted text"), ("reason", "arbitrary"), ("stage", "ready"),
])
def test_supervisor_diagnostic_rejects_unbounded_fields(field, bad_value) -> None:
    payload = guest_diagnostic_payload("guest-init", SupervisorPrivilegeError("bounding-drop", errno.EPERM, bit=5))
    payload[field] = bad_value
    payload["signature"] = guest_diagnostic_signature("key", 1, 2, payload)
    with pytest.raises(AuthenticationError):
        verify_guest_diagnostic("key", 1, 2, payload)
