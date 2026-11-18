"""Realistic workflow source probes from the Cloudflare approval re-review."""

from textwrap import dedent

import pytest
from src.services.execution.cloudflare_analysis import analyze


@pytest.mark.parametrize("sources, hosts, calls, unresolved", [
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(token: str):
                    async with httpx.AsyncClient(base_url="https://api.x.com", headers={"Authorization": f"Bearer {token}"}) as client:
                        r = await client.get("/v1/users")
                        return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="asyncclient_base_headers",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                def make_client(token):
                    return httpx.AsyncClient(base_url="https://api.x.com", headers={"Authorization": token})
                async def main(token: str):
                    client = make_client(token)
                    r = await client.get("/v1/users")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="helper_returns_client",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                def make_client(token):
                    return httpx.AsyncClient(base_url="https://api.x.com")
                async def main(token: str):
                    async with make_client(token) as client:
                        r = await client.get("/v1/users")
                        return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="helper_returns_client_with",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                class Api:
                    def __init__(self, token):
                        self.client = httpx.AsyncClient(base_url="https://api.x.com", headers={"x": token})
                    async def users(self):
                        r = await self.client.get("/v1/users")
                        return r.json()
                async def main(token: str):
                    return await Api(token).users()
            """,
        },
        ['api.x.com'], 1, 0, id="client_on_self",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    client = httpx.AsyncClient()
                    try:
                        r = await client.get("https://api.x.com/v1/users")
                        return r.json()
                    finally:
                        await client.aclose()
            """,
        },
        ['api.x.com'], 1, 0, id="try_finally_aclose",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import os, httpx
                async def main():
                    key = os.environ.get("KEY")
                    d = {}
                    d.update({"a": 1}); d.pop("a", None)
                    async with httpx.AsyncClient() as c:
                        r = await c.get("https://api.x.com/v1/x")
                    data = r.json().get("items", [])
                    ct = r.headers.get("content-type")
                    return data, ct, d.get("x"), key
            """,
        },
        ['api.x.com'], 1, 0, id="resp_json_get_env_dict",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import urllib.parse, httpx
                async def main(q):
                    qs = urllib.parse.urlencode({"q": q})
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"https://api.x.com/v1/search?{qs}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="urlencode",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(id: str):
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"https://api.x.com/v1/{id}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="fstring_id",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(path: str):
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"https://api.x.com/{path}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="fstring_origin_only",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                BASE = "https://api.x.com/v1"
                async def main():
                    async with httpx.AsyncClient() as c:
                        r = await c.get(BASE + "/users")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="module_constant_url",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                BASE = "https://api.x.com/v1"
                async def main(id):
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"{BASE}/users/{id}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="module_constant_fstring",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                BASE = "https://api.x.com"
                async def main():
                    async with httpx.AsyncClient(base_url=BASE) as c:
                        r = await c.get("/users")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="module_constant_baseurl",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                def main(q):
                    r = httpx.get("https://api.x.com/v1/search", params={"q": q})
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="httpx_get_params",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    items, page = [], 1
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        while True:
                            r = await c.get("/v1/items", params={"page": page})
                            body = r.json()
                            if not body: break
                            items += body; page += 1
                    return items
            """,
        },
        ['api.x.com'], 1, 0, id="pagination_params",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import asyncio, httpx
                async def main(ids):
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        rs = await asyncio.gather(*[c.get(f"/v1/{i}") for i in ids])
                    return [r.json() for r in rs]
            """,
        },
        ['api.x.com'], 1, 0, id="gather",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(body):
                    async with httpx.AsyncClient(timeout=30) as c:
                        r = await c.post("https://api.x.com/v1/x", json=body)
                        r.raise_for_status()
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="client_post_json",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        r = await c.request("GET", "/v1/x")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="client_request_method",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from httpx import AsyncClient
                async def main():
                    async with AsyncClient(base_url="https://api.x.com") as c:
                        r = await c.get("/v1/x")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="from_httpx_import",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(id):
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        r = await c.get(f"/v1/users/{id}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="relative_fstring_path",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(id):
                    async with httpx.AsyncClient(base_url="https://api.x.com/v1/") as c:
                        r = await c.get(f"users/{id}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="relative_fstring_noslash",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                client = httpx.AsyncClient(base_url="https://api.x.com")
                async def main():
                    r = await client.get("/v1/users")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="module_level_client",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    url = "https://api.x.com/v1/users"
                    async with httpx.AsyncClient() as c:
                        r = await c.get(url)
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="local_url_literal",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def retry(fn, *a):
                    return await fn(*a)
                async def main():
                    async with httpx.AsyncClient() as c:
                        return await retry(c.get, "https://api.x.com/a")
            """,
        },
        ['api.x.com'], 1, 0, id="retry_callback",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from pyodide.http import pyfetch
                async def main():
                    r = await pyfetch("https://api.x.com/a")
                    return await r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="pyfetch",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from js import fetch, Object
                async def main():
                    r = await fetch("https://api.x.com/a")
                    return r
            """,
        },
        ['api.x.com'], 1, 0, id="js_fetch_obj",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import urllib.request
                def main():
                    return urllib.request.urlopen("https://api.x.com/a").read()
            """,
        },
        ['api.x.com'], 1, 0, id="urllib_request",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import urllib.request
                def main():
                    req = urllib.request.Request("https://api.x.com/a", headers={"x": "y"})
                    return urllib.request.urlopen(req).read()
            """,
        },
        ['api.x.com'], 1, 0, id="urllib_request_obj",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                def main():
                    with httpx.Client(base_url="https://api.x.com") as c:
                        return c.get("/a").json()
            """,
        },
        ['api.x.com'], 1, 0, id="httpx_client_sync_with",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    async with httpx.AsyncClient() as c:
                        req = c.build_request("GET", "https://api.x.com/a")
                        r = await c.send(req)
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="httpx_build_request_send",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        async with c.stream("GET", "/big") as r:
                            return [x async for x in r.aiter_bytes()]
            """,
        },
        ['api.x.com'], 1, 0, id="stream_ctx",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from workflows.lib import client
                async def main():
                    r = await client.get("/v1/users")
                    return r.json()
            """,
            'workflows/lib.py': """
                import httpx
                client = httpx.AsyncClient(base_url="https://api.x.com")
            """,
        },
        ['api.x.com'], 1, 0, id="cross_module_client",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(ids):
                    out = []
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        for i in range(40):
                            out.append((await c.get("/v1/x", params={"i": i})).json())
                    return out
            """,
        },
        ['api.x.com'], 1, 0, id="loop_counted",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    async with httpx.AsyncClient() as client:
                        r = await client.get("https://api.x.com/a")
                    client = {"x": 1}
                    return client.get("x")
            """,
        },
        ['api.x.com'], 1, 0, id="client_get_shadow_dict",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import workers
                async def main():
                    return await workers.fetch("https://api.x.com/a")
            """,
        },
        ['api.x.com'], 1, 0, id="workers_fetch_attr",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    r = httpx.get("https://api.x.com/v1/x?a=1")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="query_in_literal",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    r = httpx.get("https://API.x.com/v1/x")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="uppercase_host",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(q):
                    r = httpx.get(f"https://api.x.com?q={q}")
                    return r.json()
            """,
        },
        ['api.x.com'], 1, 0, id="fstring_origin_query",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def fetch(client: httpx.AsyncClient, id):
                    r = await client.get(f"/v1/{id}")
                    return r.json()
                async def main(id):
                    async with httpx.AsyncClient(base_url="https://api.x.com") as c:
                        return await fetch(c, id)
            """,
        },
        ['api.x.com'], 1, 1, id="client_as_param",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                class Api:
                    def __init__(self):
                        self.client = httpx.AsyncClient(base_url="https://api.x.com")
                    async def a(self): return (await self.client.get("/a")).json()
                    async def b(self): return (await self.client.post("/b")).json()
                async def main():
                    api = Api(); return await api.a(), await api.b()
            """,
        },
        ['api.x.com'], 2, 0, id="self_client_counted",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    items = []
                    url = "https://api.x.com/v1/items"
                    async with httpx.AsyncClient() as c:
                        while url:
                            r = await c.get(url)
                            body = r.json()
                            items += body["items"]
                            url = body.get("next")
                    return items
            """,
        },
        [], 1, 1, id="pagination_loop",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(tenant):
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"https://{tenant}.x.com/v1/x")
                    return r.json()
            """,
        },
        [], 1, 1, id="fstring_tenant_host",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(path):
                    base = "https://api.x.com"
                    async with httpx.AsyncClient() as c:
                        r = await c.get(base + path)
                    return r.json()
            """,
        },
        [], 1, 1, id="url_join_var",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    async with httpx.AsyncClient() as c:
                        r = await c.get("https://api.x.com:8443/v1")
                    return r.json()
            """,
        },
        [], 1, 1, id="localhost_url",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(tenant):
                    async with httpx.AsyncClient(base_url=f"https://{tenant}.halopsa.com") as c:
                        return (await c.get("/api/tickets")).json()
            """,
        },
        [], 1, 1, id="tenant_host_fstring_base",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                GRAPH = "https://graph.microsoft.com/v1.0"
                async def main(token, user):
                    headers = {"Authorization": f"Bearer {token}"}
                    async with httpx.AsyncClient() as c:
                        r = await c.get(f"{GRAPH}/users/{user}", headers=headers)
                        r = await c.post(f"https://login.microsoftonline.com/{token}/oauth2/v2.0/token", data={})
                    return r.json()
            """,
        },
        ['graph.microsoft.com', 'login.microsoftonline.com'], 2, 0, id="graph_ms",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(token):
                    out = []
                    async with httpx.AsyncClient(headers={"Authorization": token}) as c:
                        r = await c.get("https://graph.microsoft.com/v1.0/users")
                        body = r.json()
                        out += body["value"]
                        while "@odata.nextLink" in body:
                            r = await c.get(body["@odata.nextLink"])
                            body = r.json(); out += body["value"]
                    return out
            """,
        },
        ['graph.microsoft.com'], 2, 1, id="odata_next",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                '''See https://docs.x.com/api for details.'''
                async def main():
                    return 1
            """,
        },
        [], 0, 0, id="docstring_with_url",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                DOCS = "http://example.com/docs"
                async def main():
                    return DOCS
            """,
        },
        [], 0, 0, id="url_constant_http_docs",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main():
                    return {"link": "https://portal.x.com/tickets"}
            """,
        },
        [], 0, 0, id="url_in_dict_return",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(tid):
                    return {"link": f"https://portal.x.com/tickets/{tid}"}
            """,
        },
        [], 0, 0, id="fstring_link_return",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(d: dict):
                    session = {}
                    v = session.get("x")
                    req = {"a": 1}
                    return req.get("a"), v
            """,
        },
        [], 0, 0, id="dict_get_names_like_http",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(obj):
                    return getattr(obj, "name", None)
            """,
        },
        [], 0, 0, id="getattr_plain",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import importlib.metadata
                async def main():
                    return importlib.metadata.version("httpx")
            """,
        },
        [], 0, 0, id="importlib_metadata",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(o):
                    return vars(o)
            """,
        },
        [], 0, 0, id="vars_obj",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(o):
                    return o.__dict__
            """,
        },
        [], 0, 0, id="dunder_dict",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import xml.etree.ElementTree as ET
                NS = {"s": "http://schemas.xmlsoap.org/soap/envelope/"}
                async def main(text):
                    return ET.fromstring(text).find("s:Body", NS)
            """,
        },
        [], 0, 0, id="xml_ns",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                SCHEMA = "https://json-schema.org/draft/2020-12/schema"
                async def main():
                    return {"$schema": SCHEMA}
            """,
        },
        [], 0, 0, id="https_schema_const",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from bifrost import UserError
                from bifrost.types import Thing
                async def main():
                    t = Thing(a=1)
                    if not t: raise UserError("x")
            """,
        },
        [], 0, 0, id="bifrost_types_construct",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import redis
                def main():
                    r = redis.Redis()
                    return r.get("x")
            """,
        },
        [], 0, 0, id="redis_get",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(url: str):
                    if not url.startswith("https://"):
                        url = "https://" + url
                    return url
            """,
        },
        [], 0, 0, id="startswith_https",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(url: str):
                    return url.replace("http://", "https://")
            """,
        },
        [], 0, 0, id="replace_http",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                ALLOWED = ["https://*.x.com"]
                async def main():
                    return ALLOWED
            """,
        },
        [], 0, 0, id="wildcard_host_doc",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(host):
                    return f"https://{host}"
            """,
        },
        [], 0, 0, id="fstring_https_nopath",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import logging
                log = logging.getLogger(__name__)
                async def main(host):
                    log.info(f"https://{host} contacted")
            """,
        },
        [], 0, 0, id="fstring_https_noslash_msg",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(x):
                    raise ValueError(f"Bad response from https://api.x.com: {x}")
            """,
        },
        [], 0, 0, id="error_msg_url",
    ),
    pytest.param(
        {
            'workflows/halo-psa/sync.py': """
                async def main():
                    return 1
            """,
        },
        [], 0, 0, id="hyphen_dir",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from bifrost import integrations, config, tables, workflow
                @workflow
                async def main():
                    i = await integrations.get("HaloPSA")
                    v = await config.get("api_key")
                    rows = await tables.query("t", where={"a": 1})
                    return i, v, rows
            """,
        },
        [], 3, 0, id="bifrost_sdk",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import bifrost
                async def main():
                    return await bifrost.tables.get("x")
            """,
        },
        [], 1, 0, id="bifrost_sdk_dotted",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main(url):
                    async with httpx.AsyncClient() as c:
                        return await retry(c.get, url)
            """,
        },
        [], 1, 1, id="callback_url_parameter",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                import httpx
                async def main():
                    return httpx.get("http://api.x.com/a")
            """,
        },
        [], 1, 1, id="http_only_url",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                from js import fetch
                async def main():
                    return fetch
            """,
        },
        [], 1, 1, id="unbound_http_callable",
    ),
    pytest.param(
        {
            'workflows/w.py': """
                async def main(client):
                    return await client.get("https://api.x.com/a")
            """,
        },
        [], 1, 1, id="ambiguous_client_with_url",
    ),
    pytest.param(
        {"workflows/w.py": "async def main(obj, field): return getattr(obj, field, None)"},
        [], 0, 0, id="getattr_dynamic",
    ),
    pytest.param(
        {"workflows/w.py": """
            async def main():
                data = {}
                return data.get("https://api.x.com")
        """},
        [], 0, 0, id="mapping_key_that_looks_like_a_url",
    ),
    pytest.param(
        {
            "workflows/w.py": """
                from .lib import client
                async def main():
                    return await client.get("/items")
            """,
            "workflows/lib.py": """
                import httpx
                client = httpx.AsyncClient(base_url="https://api.x.com")
            """,
        },
        ["api.x.com"], 1, 0, id="relative_workspace_client_import",
    ),
    pytest.param(
        {"workflows/w.py": """
            import httpx
            class Api:
                async def get(self):
                    return await self.client.get("/items")
                def __init__(self):
                    self.client = httpx.AsyncClient(base_url="https://api.x.com")
            async def main():
                return await Api().get()
        """},
        ["api.x.com"], 1, 0, id="client_initialized_below_other_methods",
    ),
])
async def test_review_probe(sources, hosts, calls, unresolved):
    async def read(path, max_bytes):
        source = sources.get(path)
        return None if source is None else dedent(source).encode()

    result = await analyze(read, next(iter(sources)), "main", subrequest_allowance=50, cpu_budget_ms=100_000)
    assert result["required_https_hosts"] == hosts
    assert result["estimated_subrequests"] == len(sources) + calls
    assert result["unresolved_url_calls"] == unresolved
