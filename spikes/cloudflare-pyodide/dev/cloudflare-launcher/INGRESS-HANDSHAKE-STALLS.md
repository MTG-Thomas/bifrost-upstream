# Worker Ingress TLS Handshake Stalls

The latest hosted preflight failed while starting TLS on an established IPv4 TCP connection to the Cloudflare edge. The packet-level cause remains unresolved. A bounded resolver sample from the same LXC showed TCP retransmission activity and a TLS timing tail up to 342 ms, with zero four-second timeouts. The most direct improvement is to make the existing HTTP connection pool usable and verify edge reuse while preserving the full per-address route matrix.

## Hosted Evidence

Source under review was `65b142eea` on `t3/cloudflare-pyodide-workflow-spike`. The receipts describe distinct hosted builds.

| Run | Source | Observed result |
| --- | --- | --- |
| 1 | `709e377955` | Enable stopped in `ingress_http` with `tls_error`. Admission stayed closed and the canary did not run. [Receipt](../../results/cloudflare-scaleout-hosted-attempt.json) |
| 2 | `7739c8e922` | Preliminary IPv4 TLS and HTTP passed on two addresses. Enable later stopped in `ingress_http` with `tls_error`, with both TLS diagnostic fields empty. [Receipt](../../results/cloudflare-scaleout-hosted-retry.json) |
| 3 | `eb5399823` | Worker preflight returned 100 route results. Enable and its signed canary succeeded. The subsequent execution-class stop was a harness expectation error. The Compose job runs as `remote_launcher`. [Receipt](../../results/cloudflare-scaleout-combined-hosted.json) |
| 4 | `bb05eb595` | Connect succeeded with admission closed. Worker preflight returned 29 route results and failed on `GET /api/forms/{form_id}` with IPv4 `ConnectTimeout` in `tls_handshake`, `connection_reused=false`, `handshake_completed=false` and no symbolic errno. Enable and its canary were never submitted. [Receipt](../../results/cloudflare-scaleout-combined-hosted-rerun.json) |

Loopback diagnosis showed that the old classifier could treat a nested `SSLWantReadError` as the top-level fault in either TLS or HTTP I/O. Runs 1 and 2 have insufficient phase evidence to assign the same trigger as Run 4. Run 3 followed an origin lookup single-flight fix and established that the path can pass. That fix concerns origin HTTP processing, which happens after the client-to-edge TLS negotiation that stopped Run 4. [Pilot diagnosis and combined qualification](PHASE1-PILOT.md)

The probe resolves the origin host, then connects to each eligible literal address with the origin hostname as SNI and HTTP Host. Per address, it checks one allowed route, 12 sequential refusals, then the other allowlisted method/template pairs and the encoded module variant in batches of up to eight. Source enumeration gives 87 allowed method/template pairs plus 12 refusals and one extra encoded-path case, for 100 HTTP requests per address. The receipt reports the number of results on the returned address. In Run 4, 29 is consistent with the first 13 requests plus two eight-request batches. It counts the completed batch, including the failure, and supplies no handshake ordinal. Each address has a separate WebSocket check. The HTTP client has a four-second timeout for each operation, including TCP connect and TLS separately. One 30-second deadline covers resolution, all addresses and WebSockets. A failed route stops the probe after its current batch. [Probe](../../../../api/src/services/worker_ingress_probes.py) · [Ingress policy](../../../../api/src/services/worker_ingress.py) · [Preflight wrapper](preflight-compose-worker.sh)

## Measurements

Observed November 22, 2026 UTC. The host and LXC observations used read-only commands. The public resolver was the only active target. The LXC sample ran in its network namespace outside the Compose worker, so it omitted the Docker bridge, the worker's own network namespace, the hosted SNI and certificate chain, and the hosted address and anycast path.

| Check | Hypervisor | LXC |
| --- | --- | --- |
| IPv4 ICMP echo, 200 attempts at 200 ms intervals | 200/200 replies. RTT min/mean/max/mdev 10.198/11.165/13.811/0.503 ms | 200/200 replies. RTT 10.318/11.084/14.917/0.490 ms |
| IPv4 do-not-fragment echo, 1472-byte ICMP payload | 3/3 replies | 3/3 replies |
| Relevant interfaces | Egress bridge and LXC-facing links MTU 1500 | Egress Ethernet and observed Docker bridges MTU 1500 |
| Conntrack entries before → after TLS sample | 302 → 302 of 262,144 (0.12%) | 5 → 205 of 262,144 (0.08% at endpoint) |
| Conntrack errors and drop counters | `drop`, `early_drop`, `insert_failed` all 0 and unchanged. `error` 304 and `invalid` 28,936 unchanged. Targeted kernel log scan yielded zero matches. | `conntrack` unavailable. Kernel log read denied by LXC. |
| TCP counters during sample window | Retransmitted segments +3, timeouts +2, loss probes +1, SYN retransmissions +0 | Active opens +200, retransmitted segments +16, timeouts +8, loss probes +8, SYN retransmissions +0 |
| IP discards and TCP MTU probing counters | No increase | No increase |

Both ICMP tests used `1.1.1.1`. `tracepath` was unavailable on both systems. The 1,500-byte DF test and the socket PMTU sample are the available size checks. Counters came from `nstat -asz`, the conntrack sysctls and hypervisor `conntrack -S`. They cover their namespaces and all traffic in the window. In particular, host TCP counters describe its local stack and cannot attribute retransmissions on forwarded connections.

The sole TLS burst began at 00:53:41.967 UTC and took 1.092 s. It used 200 fresh TCP sockets with eight concurrent workers, fixed IPv4 `1.1.1.1:443`, SNI `one.one.one.one`, default certificate verification, and four seconds each for connect and TLS. It sent no HTTP requests. All 200 completed non-resumed TLS 1.3 and negotiated HTTP/1.1. `ss -ti` with the resolver filter showed eight established sockets during the run and 200 in TIME-WAIT immediately after. Host snapshots bracketed the burst from 00:53:38 to 00:53:44 UTC.

| Latency (ms), nearest rank | Min | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP connect | 10.189 | 11.543 | 13.567 | 14.641 | 16.064 |
| TLS after TCP | 14.125 | 16.326 | 20.538 | 338.014 | 342.098 |
| TCP plus TLS | 24.853 | 28.168 | 32.730 | 349.724 | 354.103 |

TLS p95 was 340.157 ms in the first 20 attempts and 18.450 ms in the last 20. This was one fixed-concurrency sample, with no concurrency control. Linux `TCP_INFO` showed PMTU 1500 on all sockets, post-connect RTO 211–219 ms, and no connect retransmissions. After TLS, 192 sockets had zero local retransmissions and eight had two each, consistent with the LXC's +16 segment counter. The 192 had three outgoing and three incoming TCP data segments at TLS completion. The other eight had five outgoing and three incoming. These are socket counters, including retransmissions, with no packet capture or attribution of which bytes were delayed. The SYN, SYN-ACK, and final ACK added two outgoing and one incoming segments at TCP completion. The socket counts do not yield an unbiased on-wire packet loss rate.

## Hypothesis Disposition

### Docker-Bridge Follow-Up

At 01:03:30 UTC on November 22, the same IPv4 resolver sample ran in a throwaway container on the LXC's Docker bridge, through its NAT path. The local worker image `stage128-worker:latest` was already present. The test overrode its entry point with Python and sent only TLS handshakes. The LXC had 9,951,053 KiB available and no active Bifrost test stack at launch. Docker used the cached image and the throwaway container's removal was verified.

| Latency (ms), nearest rank | Min | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP connect | 10.301 | 11.582 | 13.625 | 15.337 | 17.181 |
| TLS after TCP | 14.525 | 17.184 | 21.847 | 54.504 | 54.672 |
| TCP plus TLS | 25.693 | 28.812 | 34.848 | 65.828 | 66.507 |

All 200 fresh connections completed in 0.824 s at eight-way concurrency, with zero four-second timeouts. Every socket negotiated non-resumed TLS 1.3 and HTTP/1.1 and reported PMTU 1500 and three TCP data segments in each direction. LXC `nstat -asz` before and after the burst showed no change in retransmissions (850), TCP timeouts (91), loss probes (264), SYN retransmissions (3), or IP discards (84 outbound, 0 inbound). The worker image lacked `nstat`, so the throwaway namespace's snapshots used the underlying kernel counters in `/proc/net/snmp` and `/proc/net/netstat`. They showed +200 TCP opens, +1,191 incoming and +1,873 outgoing TCP segments, and zero retransmissions, timeouts, loss probes, SYN retransmissions, or IP discards both before and after.

The bridge sample provides evidence against a persistent bridge-path defect under this load. It used the default Docker bridge and the public resolver, rather than the exact Compose network and the hosted SNI. The two bursts were sampled at different times. Their timing difference does not establish that the bridge improved latency.

### Findings

| Hypothesis | Disposition |
| --- | --- |
| Normal loss and TCP retransmission | **Supported as a source of shorter tails.** The resolver burst saw client retransmissions and a sub-second TLS tail. A four-second TLS timeout with an established TCP connection requires a larger or more persistent disturbance under the measured timers. Correlated loss, queueing, or handshakes delayed before edge response remain plausible. No packet trace was captured during Run 4. |
| PMTU black hole | **Disfavored for the observed resolver path.** Both DF tests passed and socket PMTU was 1500. A different anycast path, MSS or certificate flight for the hosted hostname remains untested. |
| Conntrack or NAT pressure | **Disfavored in this window.** Both measured tables were near empty and host drops and insert failures did not increase. There are no matching counters from Run 4 or from the worker bridge during its stall. An upstream NAT or transient pressure remains untested. |
| Edge handling of eight fresh handshakes from one address | **No four-second reproduction on the resolver service.** All 200 completed at the allowed concurrency. A tenant-edge-specific effect, different routing, or the worker's local scheduling remains possible. The sample has no concurrency control or Cloudflare edge telemetry for that run. |
| Client-side handshake churn | **Supported by the pinned HTTP/1.1 stack.** The probe reads headers through `client.stream()` and exits without consuming the response to end-of-message, including for expected empty responses. HTTPX closes the stream at exit. httpcore returns a socket to the pool only when both h11 states are `DONE`. Parsing response headers alone leaves the response unfinished. A shared client therefore still opens a new connection for each normal route check in this code path. The latest receipt confirms a fresh socket for the failed request. [HTTPX 0.28.1 stream lifecycle](https://github.com/encode/httpx/blob/0.28.1/httpx/_client.py#L1474-L1483) · [httpcore 1.0.9 HTTP/1.1 implementation](https://github.com/encode/httpcore/blob/1.0.9/httpcore/_async/http11.py#L154-L235) |
| Origin `Connection: close` forces edge closure | **The header alone establishes closure on the origin hop.** `Connection` is hop-by-hop and Cloudflare documents separate client and origin connections with client keep-alive. The edge can independently close the client connection. Whether it keeps this ingress's replies reusable after the client finishes reading them remains unverified. The TLS-only resolver sample contains no HTTP evidence. [RFC 9110 §7.6.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-7.6.1) · [Cloudflare connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/) |

Client-to-edge reuse remains unverified until a hosted ingress receipt records
`reused_requests > 0` for a probed address. The local TLS regression verifies
that every route also passes when the peer sends `Connection: close`, in
which case each request starts its own TCP connection. Keep the origin close
behavior because it prevents desynchronization when a refused request has an
unread body. The sanitized preflight and Enable failure details use
`tcp_connects_started` for attempts, including failed TCP connects.

## Probability of a Four-Second Timeout

Let `p` be an independent one-way packet loss probability and `q` the probability a fresh connection exhausts a four-second phase budget. For `N` independent fresh connections, the chance of at least one timeout is `1 - (1 - q)^N`. The present HTTP design has `N=100` per fully exercised address, or `N=200` when two addresses each run the complete matrix. The WebSocket uses a separate five-second budget for its combined operations and is outside this calculation. Failures of HTTP checks, incomplete address matrices and the global deadline are outside this model.

- **Measured proxy for `p`.** The LXC echo sample lost zero out of 200. Its one-sided 95% exact upper limit for round-trip echo loss is `1 - 0.05^(1/200) = 1.487%`. With independent, symmetric outbound and inbound loss, the implied upper input for `p` is `1 - 0.05^(1/400) = 0.746%`. Use the LXC sample alone because the host shares some of the path. ICMP and TCP can have different loss, and the hosted path was unobserved.
- **TCP connect.** Both namespaces report `tcp_syn_linear_timeouts=4`. In the simplified active-open model the usable SYN exchanges are at about 0, 1, 2 and 3 seconds. A SYN or SYN-ACK loss fails that exchange, so four failed exchanges gives `q_tcp = [1 - (1 - p)^2]^4`. This abstracts away the edge's independent SYN-ACK timer and uses each attempt as a fresh exchange. A single lost SYN or SYN-ACK followed by a clean exchange ordinarily costs roughly one second. [Linux TCP sysctls](https://docs.kernel.org/networking/ip-sysctl.html#tcp-syn-linear-timeouts) · [RFC 6298 §§2 and 5](https://www.rfc-editor.org/rfc/rfc6298.html)
- **TLS after connect.** With a representative measured 212 ms RTO, one repeatedly missing critical segment would be retried at about 0.21, 0.64, 1.48 and 3.18 seconds, then at 6.57 seconds under pure doubling. Five successive losses of that segment exhaust four seconds. Using all six ordinary post-connect TCP data segments counted in the resolver sample as candidate blockers, `q_tls ≈ 1 - (1 - p^5)^6`. This is a single-blocker approximation. The edge's RTO is unknown, counts reflect the resolver's TLS flight, and feedback, recovery across multiple flights, host scheduling and correlated loss can change the tail. For a deliberately slower 1 s starting RTO, the retries are at 1, 3 and 7 seconds. The analogous approximation is `q_tls ≈ 1 - (1 - p^3)^6`. A single loss still fits these representative budgets. [RFC 6298](https://www.rfc-editor.org/rfc/rfc6298.html) · [TLS 1.3 flights](https://www.rfc-editor.org/rfc/rfc8446.html#section-2)

| Assumed one-way `p` | ≥1 TLS timeout, 100 fresh sockets, 212 ms start | ≥1 TCP or TLS timeout, 100 fresh sockets, 212 ms TLS start | ≥1 TLS timeout, 100 fresh sockets, 1 s start |
| ---: | ---: | ---: | ---: |
| 0% (echo point estimate) | 0% | 0% | 0% |
| 0.1% | 0.00000000006% | 0.000000160% | 0.000060% |
| 0.5% | 0.000000188% | 0.0000992% | 0.00750% |
| 0.746% (echo-derived upper input) | 0.00000139% | 0.000490% | 0.0249% |
| 1.0% (sensitivity) | 0.00000600% | 0.00157% | 0.0600% |

The combined column uses `q = 1 - (1 - q_tcp)(1 - q_tls)`. At the echo-derived upper input it gives about 0.000980% for 200 fresh HTTP sockets. By comparison, at `p=0.1%`, the chance of at least one initially lost data segment over 100 six-segment TLS handshakes is about 45%. That much larger number describes any loss, including loss recovered inside the budget. The model makes routine independent loss a weak explanation for repeated four-second stalls, while leaving burst loss and other tail mechanisms open.

Directly, the resolver observed `0/200` four-second phase timeouts. Under independent identical attempts, its one-sided 95% upper limit for `q` is 1.487% per fresh connection, which maps to an upper limit of 77.6% over 100 or 95% over 200. Those wide limits describe what the small sample can exclude for that endpoint under the independence assumption. A single burst can share a failure condition, so these limits and the modeled rates cannot qualify the hosted path. The four historical receipts use different builds and include different failure classes. They cannot supply a hosted-rate estimate either.

## Ranked Options

1. **Product-behaviour change — reuse edge-side HTTP connections.** First measure the existing fresh and reused connection counts, response framing and client-visible closure during an authorized hosted qualification. Validate status, ingress marker and per-request nonce before reading the accepted response to end-of-message within a small byte and time budget. Close suspicious or incomplete streams. Preserve the origin close behavior, literal-address binding, SNI and hostname verification, distinct nonce per request, full route matrix, WebSocket refusal, signed canary and 30-second deadline. A fully persistent HTTP/1.1 pool could cover the sequential checks on one socket and the batches on about eight total per address, plus the separate WebSocket. Edge closure or body framing may limit savings. Verify through a controlled persistent proxy serving empty responses and through client traces on a subsequent authorized hosted run. If the edge independently closes HTTP/1.1, qualify edge HTTP/2 multiplexing with explicit ALPN, dependency and trace support before considering an origin change.
2. **Test-harness change — improve diagnostics and avoid redundant qualification traffic where the gates permit.** Record fresh TCP and TLS starts, successful reuse, timings and sanitized failure phases on every address, including successful checks. Keep the exhaustive positive and negative fence tests on the actual route inventory. A smaller hosted harness smoke can use representative allow/refusal, encoding and WebSocket checks if the unchanged Enable gate still runs the complete matrix from the same worker. This reduces qualification traffic only. Reducing the matrix inside Enable would be a separate **product-behaviour change**. It would require an owner-approved coverage argument tying the installed policy digest and full route-inventory tests to the exact deployed ingress, plus address-specific evidence for equivalent proxy routing. Every route must still be fenced by the request-time policy. Sampling loses per-address detection of route-specific proxy or rewrite drift.
3. **Infrastructure — repair a measured path defect.** Promote this to first priority if a future authorized worker-namespace observation shows PMTU loss, table exhaustion, local scheduling delay, or path drops at the failing time. Compare the worker and host observations with the same destination, phase and time window. Apply a targeted fix only where the counters or a narrowly scoped trace locate the defect. This sample gives no basis for changing MTU, conntrack capacity or firewall settings.
4. **Product-behaviour change — one narrowly bounded connect re-attempt, with owner approval.** Consider one additional attempt per entire preflight, on the same literal address and with the same SNI, for a `ConnectTimeout` explicitly classified as `tcp_connect` or `tls_handshake`, before any HTTP bytes were sent and only with sufficient time left inside the existing deadline. Record both attempts and require every route to pass. Preserve failure on certificate errors, HTTP read/write errors, wrong status, witness or nonce, and all WebSocket and canary failures. A new flow might escape a transient path problem. Correlated loss may persist, and an extra flow can worsen pressure or mask a useful signal. An unrequested fallback requires owner approval under [CLAUDE.md](../../../../CLAUDE.md#no-dead-code-no-unrequested-fallbacks).
5. **Product-behaviour change — raise only the connect-phase timeout if a measured tail justifies it.** A measured recovery just after four seconds could justify a six- to eight-second connect limit while keeping the read/write/pool budgets and overall deadline. The pinned HTTP stack uses its connect setting for both TCP and TLS, so changing only one of those phases would need an explicit transport design. Longer waits preserve the same route acceptance checks but consume the global budget and can turn an actionable per-route error into an overall deadline. The current data shows neither a recoverable tail beyond four seconds nor enough budget margin to choose a value.

**Recommendation.** Add successful-connection telemetry in the next authorized qualification and pursue bounded response completion for HTTP/1.1 reuse first. It addresses a source-verified reason for fresh handshakes while preserving all live route checks and the current admission contract. Keep the current timeout and fail-closed behavior while qualifying reuse. Escalate to targeted infrastructure diagnosis if TLS still stalls on the much smaller set of connections.

## Hosted Connection Reuse Verified

The 2026-11-22 UTC run from `97ef3ae83` passed the complete 100-route
preflight from the production Compose worker with the bounded response
drain. The counters show HTTP connection reuse through the Cloudflare
edge on both reachable addresses.

| Address family | Addresses | TCP starts per address | TLS starts and completions per address | Reused requests per address |
| --- | ---: | ---: | ---: | ---: |
| IPv4 | 2 | 8 | 8 and 8 | 92 |
| IPv6 | 2 | 1 | 0 and 0 | 0 |

Enable then passed the signed canary as `remote_launcher` and opened
admission. Its successful receipt does not expose connection counters,
so reuse was directly observed for the preflight only. The run stopped
after Enable at the local primary hold check, before any hosted workflow
submissions.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json).

## Reuse Observed in the Enable Success Receipt

The 2026-11-22 UTC run from `58029df89` recorded the same per-address
counters in the complete worker preflight and the signed Enable success
receipt. Both ran in the Compose worker as `remote_launcher`.

| Address family | Addresses | TCP starts per address | TLS starts and completions per address | Reused requests per address |
| --- | ---: | ---: | ---: | ---: |
| IPv4 | 2 | 8 | 8 and 8 | 92 |
| IPv6 | 2 | 1 | 0 and 0 | 0 |

Both checks directly observed HTTP connection reuse through the
Cloudflare edge. Enable succeeded 18.7 seconds after its job started.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).
