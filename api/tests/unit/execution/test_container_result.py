"""Exercise the guest's real byte pipe without any object deserialization."""

import os
from multiprocessing import Pipe
from queue import Empty
from struct import pack

import pytest
from src.services.execution.container_result import (
    MAX_RESULT_BYTES,
    ContainerResultReader,
    ContainerResultWriter,
    InvalidContainerResult,
)


def test_fragmented_guest_result_does_not_block_the_supervisor() -> None:
    received, sent = Pipe(duplex=False)
    with received, sent:
        reader = ContainerResultReader(received)
        data = b'{"success":true,"duration_ms":3,"metrics":{"peak_memory_growth_bytes":1}}'
        header = pack("!I", len(data))
        os.write(sent.fileno(), header[:2])
        with pytest.raises(Empty):
            reader.get_nowait()
        os.write(sent.fileno(), header[2:] + data[:3])
        with pytest.raises(Empty):
            reader.get_nowait()
        os.write(sent.fileno(), data[3:])
        assert reader.get_nowait() == {"success": True, "duration_ms": 3}


@pytest.mark.parametrize("data", [
    pack("!I", MAX_RESULT_BYTES + 1),
    pack("!I", 1) + b"\xff",
    pack("!I", 2) + b"{}",
])
def test_guest_pipe_rejects_invalid_frames(data: bytes) -> None:
    received, sent = Pipe(duplex=False)
    with received, sent:
        reader = ContainerResultReader(received)
        os.write(sent.fileno(), data)
        with pytest.raises(InvalidContainerResult):
            reader.get_nowait()


def test_guest_cannot_report_a_nonterminal_state_as_its_result() -> None:
    received, sent = Pipe(duplex=False)
    with received, sent:
        reader = ContainerResultReader(received)
        ContainerResultWriter(sent).send({"success": True, "duration_ms": 1, "status": "Running"})
        with pytest.raises(InvalidContainerResult, match="terminal status"):
            reader.get_nowait()


def test_guest_writer_and_reader_preserve_json_values() -> None:
    received, sent = Pipe(duplex=False)
    with received, sent:
        reader = ContainerResultReader(received)
        outcome = {"success": True, "duration_ms": 10, "result": {"quality": 0.9, "values": [None, "été"]}}
        ContainerResultWriter(sent).send(outcome)
        assert reader.get_nowait() == outcome
