"""Run inside the trial API image to read its plan and registry inventory."""

import ast
import asyncio
import inspect
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from types import SimpleNamespace
from uuid import UUID


def source_tree(function):
    return ast.parse(dedent(inspect.getsource(function)))


def call_from(function, name):
    return next(node for node in ast.walk(source_tree(function))
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name)


def assignment_from(function, name):
    return next(node.value for node in ast.walk(source_tree(function))
                if isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name)


def render(expression, **bindings):
    # These expressions come from the trusted product image. The receipt
    # supplies data bindings only. Keep release-specific naming out of the runner.
    return eval(compile(ast.Expression(expression), "<product naming>", "eval"), {"__builtins__": {}}, bindings)


def plan(receipt):
    from src.core.security import decrypt_secret
    from src.jobs.platform.cloudflare_enable import _deploy_container_launcher, _settle_enable
    from src.services.worker_pool.cloudflare_containers_rest import REGISTRY, ContainerRegistry, _name

    data = json.loads(decrypt_secret(receipt))
    assert data["purpose"] == "cloudflare-activation-v1" and data["containers_release"]
    installation = UUID(data["installation_id"])
    revision = 1  # Initial revision created by the activation endpoint.
    prefix = render(assignment_from(_settle_enable, "prefix"), row=SimpleNamespace(id=installation),
                    payload=SimpleNamespace(revision=revision))
    deployment = call_from(_settle_enable, "WorkersDeployment")
    gateway, = [item.value for item in deployment.keywords if item.arg == "gateway_name"]
    scripts = [render(expression, prefix=prefix) for expression in (*deployment.args[2:4], gateway)]
    application = render(assignment_from(_name, "name"),
                         target=SimpleNamespace(installation_id=installation),
                         intent=SimpleNamespace(desired_revision=revision))
    launcher = next(node.value for node in ast.walk(source_tree(_deploy_container_launcher))
                    if isinstance(node, ast.keyword) and node.arg == "name" and isinstance(node.value, ast.JoinedStr))
    scripts.append(render(launcher, name=application))
    destination = render(assignment_from(ContainerRegistry.copy, "destination"), REGISTRY=REGISTRY,
                         account=data["account_id"], name=application)
    registry, repository, tagged = destination.split("/")
    name, tag = tagged.rsplit(":", 1)
    assert registry == REGISTRY and repository == data["account_id"] and name == application
    return {"installation": str(installation), "account": data["account_id"], "scripts": scripts,
            "application": application, "registry_tag": tag, "registry_repository": name}


async def registry(account, token):
    from src.services.worker_pool import cloudflare_containers_rest

    adapter = cloudflare_containers_rest.ContainerRegistry(None)
    tool = cloudflare_containers_rest.run_oci_tool
    env = {"CLOUDFLARE_ACCOUNT_ID": account, "CLOUDFLARE_API_TOKEN": token}
    with TemporaryDirectory(prefix="bifrost-trial-registry-") as temporary:
        cwd = Path(temporary)
        assert (await tool(adapter.cf, "--version", cwd=cwd)).strip() == b"1.0.0-beta.31"
        owner = json.loads(await tool(adapter.cf, "accounts", "get", "-q", cwd=cwd, env=env))
        assert owner["id"] == account
        return json.loads(await tool(adapter.cf, "containers", "images", "list", cwd=cwd, env=env))


if __name__ == "__main__":
    request = json.load(sys.stdin)
    result = plan(request["receipt"]) if "receipt" in request else asyncio.run(registry(**request))
    print(json.dumps(result))
