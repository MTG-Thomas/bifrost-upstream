"""Build deterministic, secret-free install ZIPs using a strict source allowlist.

Run: py -3 examples/github-app/build.py --output <scratch-directory>
No server, registration, credentials or deployment is involved.
"""

import argparse
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ROOT = Path(__file__).resolve().parent


def identity(name):
    return str(uuid5(NAMESPACE_URL, f"https://github.com/MTG-Thomas/bifrost-upstream/github-app/{name}"))


def declarations(kind, *, enable_ingress=False, enable_reconciliation=False):
    names = ("workflows", "tables", "events", "connections", "forms") if kind == "operator" else ("workflows", "tables", "events")
    sections = {name: json.loads((ROOT / kind / ".bifrost" / f"{name}.yaml").read_text(encoding="utf-8"))[name]
                for name in names}
    if kind == "operator":
        for event in sections["events"].values():
            if event["source_type"] == "webhook":
                event["is_active"] = enable_ingress
            elif event["source_type"] == "schedule":
                event["is_active"] = event["schedule_enabled"] = enable_reconciliation
    return sections


def files(kind, *, enable_ingress=False, enable_reconciliation=False):
    entries = {"bifrost.solution.yaml": (ROOT / kind / "bifrost.solution.yaml").read_text(encoding="utf-8")}
    for name, body in declarations(kind, enable_ingress=enable_ingress, enable_reconciliation=enable_reconciliation).items():
        entries[f".bifrost/{name}.yaml"] = json.dumps({name: body}, indent=2) + "\n"
    sources = ("modules/github_app.py", "workflows/operator.py") if kind == "operator" else ("workflows/receipt.py",)
    for name in sources:
        entries[name] = (ROOT / kind / name).read_text(encoding="utf-8")
    entries["README.md"] = (ROOT / "README.md").read_text(encoding="utf-8")
    return entries


def build(output, *, enable_ingress=False, enable_reconciliation=False):
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for kind in ("operator", "consumer"):
        destination = output / f"github-app-{kind}-0.1.0.zip"
        with ZipFile(destination, "w", ZIP_DEFLATED) as archive:
            for name, content in sorted(files(kind, enable_ingress=enable_ingress, enable_reconciliation=enable_reconciliation).items()):
                info = ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                archive.writestr(info, content)
        results.append(destination)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--enable-ingress", action="store_true")
    parser.add_argument("--enable-reconciliation", action="store_true")
    args = parser.parse_args()
    for result in build(args.output, enable_ingress=args.enable_ingress, enable_reconciliation=args.enable_reconciliation):
        print(result)
