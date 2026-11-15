# Workers for Platforms probe

Two questions decide whether Workers for Platforms (WfP) is worth buying for
per-tenant engine children, and Cloudflare's docs answer neither:

1. Does a **Python user Worker in a dispatch namespace** cold-start from a
   deploy-time memory snapshot, as a normally deployed Python Worker does?
2. Can its CPU per invocation be raised past the **30 s** default, to the 5 min
   a normal Worker allows (`limits.cpu_ms` on the script, `limits.cpuMs` on
   `DISPATCHER.get`)?

`user.py` is a Python Worker carrying the full engine runtime
(`dev/runtime/generated-runtime.mjs`, imported at top level). `dispatch.mjs` is
the dispatch Worker. `wfp-lab.mjs` builds, deploys and measures both.

## Signals (calibrated on a normal deploy, 2026-11-15)

- **Snapshot:** snapshot creation runs top-level code with the clock at the Unix
  epoch, so `module_wall_ms` reads **461–462** when the isolate was restored
  from a snapshot; without one, top-level code runs inside the first request and
  reads real wall time. `cold` redeploys a new version each round (forcing new
  isolates) and reports `snapshot: true|false` per round.
- **CPU:** `burn --n N` runs a pure Python loop; wall time ≈ CPU time. A Worker
  over its CPU limit fails the request instead of returning 200.

Calibration on a normally deployed copy of `user.py` (same runtime, 11.7 MB
upload, deploy-time startup 2.0–2.6 s):

| Case | Result |
|---|---|
| `cold`, 3 rounds, new version each | snapshot **3/3** (`module_wall_ms` 461); first request 1 329 / 1 167 / 499 ms; warm 27–46 ms |
| `burn --n 800000000`, no `cpu_ms` | **500 `1101` after 35.7 s** (the 30 s default cap) |
| same, `cpu_ms: 300000` | **200 after 67.7 s** |

So both instruments distinguish the outcomes they are meant to. Rate: ~11–12 M
loop iterations per CPU second.

## Run it once WfP is enabled

Every lab Worker needs `x-lab-auth` (secret in `%TEMP%/wfp-lab-secrets.json`)
and answers 403 after `LAB_EXPIRES_AT`.

```powershell
$env:LAB_EXPIRES_AT = [DateTimeOffset]::UtcNow.AddDays(2).ToUnixTimeMilliseconds()
$lab = 'dev/staging/wfp-lab/wfp-lab.mjs'; $ns = 'bifrost-wfp-lab-ddf316c1'
$u = "$env:TEMP/wfp-user"; $d = "$env:TEMP/wfp-dispatch"   # build dirs, outside the repo
cf workers-for-platforms dispatch-namespaces create --name $ns
node $lab build-user --out $u --name wfp-lab-user --cpu-ms 300000
node $lab deploy --dir $u --dispatch-namespace $ns
node $lab build-dispatch --out $d --name bifrost-wfp-lab-dispatch-ddf316c1 --namespace $ns --user-script wfp-lab-user
node $lab deploy --dir $d
$url = 'https://bifrost-wfp-lab-dispatch-ddf316c1.<subdomain>.workers.dev/'
node $lab cold --dir $u --url $url --rounds 4 --dispatch-namespace $ns   # Q1
node $lab burn --url $url --n 800000000                                 # Q2, script limit only
node $lab burn --url $url --n 800000000 --cpu-ms 300000                 # Q2, plus dispatcher limit
```

A first deploy of a brand-new script with `--secrets-file` can fail with
`10007` (the CLI uploads the secret before the script exists); deploying again
succeeds.
