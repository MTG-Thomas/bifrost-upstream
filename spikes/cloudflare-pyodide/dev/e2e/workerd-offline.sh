#!/bin/sh
set -eu
if [ "$1" = serve ]; then
  if [ ! -r "$REHEARSAL_PYODIDE_CACHE_DIR/pyodide_314.0.6_2026-08-17_6.capnp.bin" ]; then
    echo 'Verified offline Pyodide bundle is missing at workerd startup' >&2
    exit 1
  fi
  exec "$REHEARSAL_WORKERD_BINARY" "$@" --pyodide-bundle-disk-cache-dir="$REHEARSAL_PYODIDE_CACHE_DIR"
fi
exec "$REHEARSAL_WORKERD_BINARY" "$@"
