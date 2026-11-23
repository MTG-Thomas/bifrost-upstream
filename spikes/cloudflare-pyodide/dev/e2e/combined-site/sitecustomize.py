"""Bind the runbook's Cloudflare endpoints to the disposable local fixture."""

import importlib.abc
import importlib.machinery
import os
import ssl
import sys

ADDRESS = os.environ["REHEARSAL_ADDRESS"]
PROVIDER = f"http://{ADDRESS}:{os.environ['REHEARSAL_PROVIDER_PORT']}"
EDGE_PORT = int(os.environ["REHEARSAL_EDGE_PORT"])
CANARY_HOSTS = {"existing.example.test", os.environ["REHEARSAL_MANAGED_HOST"]}


def capacity_transport(httpx):
    send = httpx.AsyncClient.send

    async def send_local(self, request, *args, **kwargs):
        host = request.url.host
        if host.endswith(".rehearsal.workers.dev"):
            script = host.removesuffix(".rehearsal.workers.dev")
            request.url = httpx.URL(PROVIDER).copy_with(
                raw_path=f"/edge-capacity/{script}".encode() + request.url.raw_path,
            )
        elif host == "api.cloudflare.com" or host.endswith(".workers.dev"):
            raise RuntimeError("Real Cloudflare endpoint refused in local rehearsal")
        return await send(self, request, *args, **kwargs)

    httpx.AsyncClient.send = send_local


def ingress_transport(probes):
    probe_address = probes._probe_address

    async def local_addresses(host):
        if host not in CANARY_HOSTS:
            raise ValueError("Unexpected local rehearsal origin")
        return (ADDRESS,)

    async def local_probe(origin, host, address, *, progress=None):
        assert address == ADDRESS and host in CANARY_HOSTS
        return await probe_address(
            origin, host, address, EDGE_PORT,
            trust=ssl.create_default_context(cafile="/rehearsal/edge.pem"), progress=progress,
        )

    probes._public_addresses = local_addresses
    probes._probe_address = local_probe


def rest_transport(rest):
    rest.API = PROVIDER + "/client/v4"


HOOKS = {
    "httpx": capacity_transport,
    "src.services.worker_ingress_probes": ingress_transport,
    "src.services.worker_pool.cloudflare_workers_rest": rest_transport,
}


class FixtureLoader(importlib.abc.Loader):
    def __init__(self, loader, change):
        self.loader = loader
        self.change = change

    def create_module(self, spec):
        return self.loader.create_module(spec)

    def exec_module(self, module):
        self.loader.exec_module(module)
        self.change(module)


class FixtureFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname in HOOKS:
            # The product imports its dependencies after Python startup.
            spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
            spec.loader = FixtureLoader(spec.loader, HOOKS[fullname])
            return spec
        return None


sys.meta_path.insert(0, FixtureFinder())
