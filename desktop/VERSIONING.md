# Desktop wrapper versioning (hosted-only Electron shell).
#
# Rule: the desktop shell version TRACKS but never COUPLES to releases.
# - Tagged releases (`vX.Y.Z`, e.g. `v0.8.0`): desktop build injects the tag
#   version via `-c extraMetadata.version=<tag>` (see desktop-build.yml).
#   Committed `desktop/package.json` stays `0.0.0` — never bump it per release.
# - Dev/untagged builds: version stays `0.0.0-dev`/placeholder; the shell's
#   About screen shows the server's `VITE_BIFROST_VERSION` at runtime instead,
#   so a stale shell never misreports the hosted backend version.
# - No coupling: tagging a server/client release does NOT require a desktop
#   rebuild, and shipping a shell fix does NOT require a server release.
#   Rebuild desktop only when the shell itself changes; push the new
#   installer + `latest.yml` to the update feed and bump the feed, not git tags.
#
# Open: whether shell fixes get their own `desktop-vX.Y.Z` tags (proposed: no,
# feed `latest.yml` is the source of truth; tags only for the server/client).
