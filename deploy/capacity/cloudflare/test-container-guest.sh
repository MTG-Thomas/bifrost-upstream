#!/usr/bin/env bash
# The caller supplies a local API test image, built with the matched dependency lock.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo 'Usage: ./test.sh cloudflare-containers <local-api-image>' >&2
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
trap 'docker image rm "$tag" >/dev/null 2>&1 || true' EXIT
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
