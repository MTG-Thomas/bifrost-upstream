#!/usr/bin/env bash
# The caller supplies a local API test image, built with the matched dependency lock.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || (${2:-} && ${2:-} != --miniflare) ]]; then
    echo 'Usage: ./test.sh cloudflare-containers <local-api-image> [--miniflare]' >&2
    exit 2
fi
base=$1
docker image inspect "$base" >/dev/null
if [[ ! $(docker buildx version) =~ [[:space:]]v0\.21\.3[[:space:]] ]]; then
    echo 'Container guest conformance requires Buildx v0.21.3.' >&2
    exit 2
fi
case "$(docker compose ls --format json)" in
    '[]') ;;
    *) echo 'Container guest conformance requires an idle Docker host.' >&2; exit 2 ;;
esac
if ! free -g | awk '/^Mem:/ { if ($7 >= 8) ready=1 } END { if (!ready) exit 1 }'; then
    echo 'Container guest conformance requires 8 GB available at startup.' >&2
    exit 2
fi

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$repo"
tag="bifrost-containers-test:$(git rev-parse --short=12 HEAD)-$$"
proxy="bifrost-containers-proxy:${tag#*:}"
fixture=
trap 'docker image rm "$tag" "$proxy" >/dev/null 2>&1 || true; if [[ -n "$fixture" ]]; then rm -rf "$fixture"; fi' EXIT
docker buildx build --builder default --load --pull=false --network=none \
    --build-arg "BIFROST_API_IMAGE=$base" -f deploy/capacity/cloudflare/Dockerfile.containers \
    -t "$tag" deploy/capacity/cloudflare

mounts=(
    --mount "type=bind,src=$PWD/api/src,dst=/app/src,readonly"
    --mount "type=bind,src=$PWD/api/shared,dst=/app/shared,readonly"
    --mount "type=bind,src=$PWD/api/bifrost,dst=/app/bifrost,readonly"
    --mount "type=bind,src=$PWD/api/qualification/cloudflare_containers,dst=/app/qualification,readonly"
)
for scenario in isolation cancel launcher; do
    docker run --pull=never --rm --network=none "${mounts[@]}" \
        --entrypoint /usr/local/bin/python "$tag" /app/qualification/docker_guest.py "$scenario"
done

if [[ ${2:-} == --miniflare ]]; then
    # Miniflare looks up the sidecar by local tag. Materialize that tag from
    # the pinned digest without relying on Docker retaining tag@digest aliases.
    source=cloudflare/proxy-everything@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8
    docker image inspect "$source" >/dev/null 2>&1 || docker pull --platform linux/amd64 "$source"
    docker image tag "$source" "$proxy"
    fixture=$(mktemp -d)
    cp deploy/capacity/cloudflare/registry-toolchain/package*.json "$fixture/"
    cp deploy/capacity/cloudflare/container-miniflare.mjs "$fixture/"
    docker run --rm --pull=never --network host --workdir /probe \
        --mount "type=bind,src=$fixture,dst=/probe" \
        --mount "type=bind,src=$repo/deploy/capacity/cloudflare,dst=/fixture,readonly" \
        --mount "type=bind,src=$repo/api/src/services/worker_ingress_routes.json,dst=/fixture-routes.json,readonly" \
        --mount type=bind,src=/run/docker.sock,dst=/run/docker.sock \
        --mount type=bind,src=/etc/ssl/certs,dst=/etc/ssl/certs,readonly \
        --entrypoint /bin/sh node:26.13.1-bookworm-slim \
        -c 'npm ci --omit=dev --ignore-scripts --no-audit --no-fund && node container-miniflare.mjs "$@"' sh "$tag" "$proxy"
fi
