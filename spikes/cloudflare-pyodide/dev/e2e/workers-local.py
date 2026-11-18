"""Drive the workerd provider's loopback-only conformance fixture."""

import json
import os
import sys
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def main() -> None:
    url = os.environ["CAPACITY_HOOK_URL"]
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.path != "/hook":
        raise ValueError("Workers conformance hook requires the local fixture")
    body = json.load(sys.stdin)
    body["action"] = sys.argv[1]
    request = Request(url, json.dumps(body).encode(), {
        "Authorization": f'Bearer {os.environ["CAPACITY_TOKEN"]}',
        "Content-Type": "application/json",
    })
    with urlopen(request, timeout=15) as response:
        print(response.read().decode())


if __name__ == "__main__":
    main()
