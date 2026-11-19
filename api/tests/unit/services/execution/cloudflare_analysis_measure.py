"""Measure the Linux child on bounded synthetic source, without the API or pytest."""

import json
import os
import subprocess
import sys
from time import monotonic

CASES = {
    "concat": b'import httpx\npart = "' + b"a" * 2400 + b'"\nhttpx.get(part + part)\n',
    "f-string": b'import httpx\npart = "' + b"a" * 2400 + b'"\nhttpx.get(f"{part}{part}")\n',
    "dense-list": b"values = [" + b"1," * 20_000 + b"]\n",
    "dense-token-refusal": b"values = [" + b"1," * 38_000 + b"]\n",
    "nested-scopes": b"".join(b" " * n + b"def f():\n" for n in range(48)) + b" " * 48 + b"pass\n",
    "large-literal": b'import httpx\nhttpx.get("' + b"a" * (160 * 1024) + b'")\n',
}


def measure(worker: str, shape: str) -> dict[str, object]:
    source = CASES[shape]
    started = monotonic()
    with subprocess.Popen(
        [sys.executable, "-I", worker], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        env={},
    ) as process:
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write(b'{"diagnostics":true,"path":"entry.py","function_name":null,"single_file":true}\n')
        process.stdin.flush()
        request = json.loads(process.stdout.readline())
        assert request["read"] == "entry.py" and len(source) <= request["maximum"]
        process.stdin.write(json.dumps({"size": len(source)}).encode() + b"\n")
        process.stdin.write(source)
        process.stdin.close()
        response = json.loads(process.stdout.readline())
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
        assert process.returncode == 0
    return {
        "case": shape,
        "bytes": len(source),
        "rules": [item["rule_id"] for item in response["result"]],
        "wall_s": monotonic() - started,
        "cpu_s": usage.ru_utime + usage.ru_stime,
        "peak_rss_mib": usage.ru_maxrss / 1024,
    }


if __name__ == "__main__":
    print(json.dumps([measure(sys.argv[1], shape) for shape in sys.argv[2:] or CASES]))
