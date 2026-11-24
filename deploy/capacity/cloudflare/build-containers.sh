#!/usr/bin/env bash
set -euo pipefail

# Invoked by release CI with an authenticated GHCR client and cosign OIDC.
base=$1
repository=$2
revision=$3
output=$4
rates=$5
[[ "$base" =~ ^ghcr\.io/[^@]+@sha256:[a-f0-9]{64}$ ]]
[[ "$repository" =~ ^ghcr\.io/[a-z0-9._/-]+$ ]]
[[ "$revision" =~ ^[a-f0-9]{40}$ ]]
test "$(git rev-parse HEAD)" = "$revision"
mkdir -p "$output"
cat deploy/capacity/cloudflare/container-registry.mjs deploy/capacity/cloudflare/container-scope.mjs \
    > "$output/container-registry.mjs"
metadata=$(mktemp)
trap 'rm -f "$metadata"' EXIT
workers_digest=$(docker run --rm --network=none --platform linux/amd64 --entrypoint /usr/local/bin/python "$base" \
    -c 'import sys
from src.services.worker_pool.cloudflare_containers_release import ContainerRates
from src.services.worker_pool.cloudflare_release import load_workers_release
ContainerRates.model_validate_json(sys.argv[1])
print(load_workers_release().digest)' "$rates")
[[ "$workers_digest" =~ ^[a-f0-9]{64}$ ]]
docker buildx build --platform linux/amd64 --provenance=false --sbom=false \
    --build-arg "BIFROST_API_IMAGE=$base" --label "org.opencontainers.image.revision=$revision" \
    --file deploy/capacity/cloudflare/Dockerfile.containers \
    --output "type=image,name=$repository:sha-$revision,push=true,oci-mediatypes=true" \
    --metadata-file "$metadata" deploy/capacity/cloudflare
image_digest=$(jq -er '."containerimage.digest" | select(test("^sha256:[a-f0-9]{64}$"))' "$metadata")
wheel_digests=$(docker run --rm --network=none --platform linux/amd64 --entrypoint /bin/cat \
    "$repository@$image_digest" /opt/bifrost-wheels/installed.json |
    jq -ce '.install | map(.download_info |
        if (.url | tostring | test("^file:///opt/bifrost-wheels/[^/]+\\.whl$")) != true
            or (.archive_info.hashes.sha256 | tostring | test("^[a-f0-9]{64}$")) != true
        then error("Installed wheel lacks a trusted local URL or SHA-256")
        else {key: (.url | split("/")[-1]), value: .archive_info.hashes.sha256}
        end
    ) | from_entries')
module_digest=$(sha256sum "$output/container-registry.mjs" | cut -d' ' -f1)
jq -e -nS --arg revision "$revision" --arg repo "$repository" --arg digest "$image_digest" \
    --arg workers "$workers_digest" --arg module "$module_digest" --argjson rates "$rates" \
    --argjson wheels "$wheel_digests" '{
  manifest_version: 1, source_revision: $revision, repository: $repo, image_digest: $digest,
  platform: "linux/amd64", instance_type: "standard-1",
  entrypoint: ["/usr/local/bin/python", "-m", "src.worker.remote.container_entrypoint"],
  capacity_protocol: 2, sdk_protocol: 1, ingress_contract: "worker-ingress-only-v1",
  workers_release_digest: $workers, provider_modules: {"container-registry.mjs": $module},
  wheels: $wheels, rates: $rates
}' > "$output/manifest.json"
cosign sign-blob --yes --new-bundle-format --bundle "$output/manifest.sigstore.json" "$output/manifest.json"
