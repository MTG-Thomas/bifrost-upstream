# Workload token exchange

CI and other workloads need a short-lived BiFrost access token without a person,
a stored password, or a long-lived refresh token. Password login is MFA-gated,
refresh tokens rotate, and workflow endpoint keys are a different proposal.
This note is the incremental design for inbound workload authentication. It
follows the direction in [gobifrost/bifrost#1311](https://github.com/gobifrost/bifrost/issues/1311):
OAuth 2.0 Token Exchange (RFC 8693) on a shared path, with GitHub Actions and
Azure managed identities as acceptance cases rather than the platform model.
It is a design direction, not an upstream commitment.

## Where the grant lives

BiFrost already publishes an OAuth 2.1 authorization server for MCP. Its
metadata is `GET /.well-known/oauth-authorization-server` and its token
endpoint is `POST /token` (`BifrostAuthProvider` in
`api/src/services/mcp_server/auth.py`). That endpoint is the RFC 8414
`token_endpoint`. It already accepts `authorization_code` and `refresh_token`
and returns a human session.

RFC 8693 is a grant on the token endpoint, not a new login API. The exchange
grant is dispatched there, before the human grants, and is the only grant that
can credential a non-human identity:

`grant_type=urn:ietf:params:oauth:grant-type:token-exchange`

`POST /auth/login`, `POST /auth/refresh`, and the device flow stay human-only.
They still refuse identities. The exchange never creates a `human_auth_sessions`
row and never returns a refresh token. Authorization-server metadata advertises
the new grant and the scope `bifrost:api` beside the existing MCP grants.
Protected-resource metadata for `/mcp` is unchanged.

## Trust rules

An administrator configures trust. Nothing is trusted until a rule exists.
Rules are platform-scoped rows in `workload_trust_rules`, managed at
`/api/settings/workload-trust` by a caller with `settings.read` or
`settings.readwrite` at the platform boundary. Creates, updates, and deletes
are audited. There is no public directory of rules.

Each rule stores:

| Field | Meaning |
| --- | --- |
| `provider` | `github_actions` or `azure_managed_identity` |
| `enabled` | Disabled rules never match |
| `issuer` | Exact `iss` the subject token must carry |
| `subject_token_type` | Fixed by the provider profile |
| `audiences` | One to four accepted `aud` values; the token must match one |
| `jwks_uri` or `pinned_jwks` | Exactly one. Pinned keys are used as-is and are never fetched |
| `claim_conditions` | Mandatory exact string matches, all of which must hold |
| `identity_id` | An existing BiFrost identity (`users.identity_kind` set) |
| `token_lifetime_seconds` | Issued access-token lifetime. Default 900. Minimum 60. Maximum 3600 |

A rule can name only an active identity. Deleting that user is restricted by
the foreign key. A later deactivation or loss of identity status fails the
exchange closed. At most 100 rules may exist. Two enabled rules that both
match the same token are a configuration error: the exchange fails closed
instead of picking one.

Claim conditions are `{claim, equals}` pairs. `equals` is a non-empty exact
string. There is no wildcard, prefix, regex, or empty match. Missing claims
do not match. Non-string claims do not match. `iss`, `aud`, `exp`, `iat`,
`nbf`, and `jti` are not conditions; issuer, audience, and time are checked
on their own. Comparison of condition values uses a constant-time compare.

Pinned JWKS documents may contain only public keys (`RSA`, `EC`, or `OKP`).
A private `d` parameter is rejected at save time.

## Subject token types

The contract is not ID-token-only. Two types are accepted. Any other
`subject_token_type` is `invalid_request` and does not look up rules.

### `urn:ietf:params:oauth:token-type:id_token`

JSON Web Token ID tokens. The GitHub Actions profile is the acceptance case.

GitHub Actions OIDC tokens are issued by
`https://token.actions.githubusercontent.com`. A rule's issuer must be that
exact value. Its canonical JWKS URL is
`https://token.actions.githubusercontent.com/.well-known/jwks`. A rule either
pins keys or sets that URL; it cannot point JWKS discovery at another host.

Required conditions: `repository` (`owner/name`) and `sub`. Optional
conditions, each exact: `ref`, `job_workflow_ref`, `environment`,
`repository_owner`, `actor`, `sha`, `workflow_ref`, `runner_environment`,
`event_name`. The verified `sub` must also start with `repo:{repository}:`.
A token with no `environment` claim does not satisfy an `environment`
condition. That is how a rule limits a deployment to one environment without
treating a missing claim as "any environment".

### `urn:ietf:params:oauth:token-type:jwt`

JWTs that are not ID tokens. Azure managed-identity access tokens are the
acceptance case. They are Entra-issued access tokens, so `id_token` would be
the wrong type.

The rule's issuer must be exactly one of:

- `https://login.microsoftonline.com/{tenant-id}/v2.0`
- `https://sts.windows.net/{tenant-id}/`

`{tenant-id}` is a GUID. IMDS and the App Service identity endpoint often
return the v1 issuer (`sts.windows.net`) with `ver` `1.0`. v2 tokens use the
login.microsoftonline.com issuer and `ver` `2.0`. If `ver` is present it must
be one of those two values.

Required conditions: `tid` and `oid`. `tid` must equal the tenant in the
issuer. `oid` is the managed identity's object id. Optional exact conditions:
`appid` (the identity's application id), `sub`, and `xms_mirid`. Requiring
`appid` is recommended and not mandatory in this increment; see open questions.

The token `aud` is the resource the workload requested (an application ID URI
or application id that represents this BiFrost instance). The rule lists the
accepted audiences. Canonical JWKS URLs, when keys are not pinned, are:

- v2: `https://login.microsoftonline.com/{tenant-id}/discovery/v2.0/keys`
- v1: `https://login.microsoftonline.com/{tenant-id}/discovery/keys`

### Adding a provider

Add a provider profile next to the two above. A profile fixes the
`subject_token_type`, the allowed issuer shape, the required and optional
condition names, the canonical JWKS URL, and any extra checks after signature
verification. Register it in the profile map. Add a thin client helper that
only obtains that provider's token and posts it here. Do not add a database
enum and do not branch the exchange core on the provider name beyond calling
the profile. A rule whose provider is no longer registered fails closed.

## Validation

Both token types are JWTs. Verification uses PyJWT and `cryptography`, which
are already dependencies. The steps are fail closed:

1. Reject a missing token, a token larger than 16 KiB, or a token that is not
   a compact JWT.
2. Allow only `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`, `ES256`,
   `ES384`, and `ES512`. `none` and HMAC algorithms are rejected.
3. Verify the signature against the rule's pinned keys or its fetched JWKS.
   The `kid` selects a key; if it is absent, each public key is tried. A
   private key in a JWKS is ignored.
4. Require `iss`, `aud`, `sub`, `exp`, and `iat`. Check issuer and audience
   for equality. Allow 60 seconds of clock skew on `exp`, `nbf`, and `iat`.
5. Apply the rule's claim conditions and the profile's extra checks.
6. Require exactly one matching enabled rule.

Disabled rules are skipped. A wrong issuer, wrong audience, bad signature, or
expired token does not match. The client always receives the same
`invalid_grant` description. The audit event records the specific reason.

JWKS fetch, used only when the rule has a `jwks_uri` and no pinned keys:

- HTTPS on port 443, no userinfo, no redirects, no environment proxy.
- DNS answers must all be publicly routable. Loopback, private, link-local
  (including the cloud metadata addresses), and non-global addresses are
  rejected.
- Timeout 5 seconds. Body cap 64 KiB.
- Successful documents are cached in process for 10 minutes. Failures are not
  cached and do not issue a token.
- Provider profiles additionally require their canonical JWKS host, so an
  administrator cannot aim discovery at an internal URL.

There is a time-of-check/time-of-use window between DNS resolution and the
TLS connection. The profile host pin, the public-address check, and pinned
keys are the controls. Tests use pinned keys and do not touch the network.

The subject token is an input. It is never copied into the issued token,
the response, or a log line.

## Identity, lifetime, and scope

A successful exchange calls `issue_workload_access_token` for the rule's
identity. That is a separate minter from `issue_access_token`. The human
minter refuses every identity (`identity_kind` set) and will continue to.
The workload minter requires an active identity, reads the same authorization
snapshot and authority epoch as a human access token, and refuses anyone else.

The issued JWT is a `ver: 1` access token with `credential_kind` `workload`,
the identity's compressed `usr` claims (including `kind`), and no session id.
API authentication accepts it on its own branch. It does not fall through to
human parsing, which still rejects identities, and it is not a runtime
credential, which still requires a live run. The principal carries that
identity's roles and permissions. A permission or identity change bumps the
authority epoch and the token stops working. There is no refresh grant for
it; the workload exchanges again.

`requested_token_type`, when sent, must be
`urn:ietf:params:oauth:token-type:access_token`. Anything else is
`invalid_target`. `actor_token` is rejected with `invalid_request`. This
increment does not implement delegation or impersonation.

The only scope is `bifrost:api`. Omitting `scope` requests that scope.
Any other scope is `invalid_scope`. The scope does not grant permissions.
The identity's roles do. A rule must not point at an identity that holds
Platform Admin unless that is the intended authority.

Lifetime comes from the rule, clamped to 60–3600 seconds, default 15 minutes.
The issued token's `aud` is BiFrost's own API audience, not the external
token's audience.

## Response and errors

Success is HTTP 200 with `Cache-Control: no-store`, as a token response:

```json
{
  "access_token": "<BiFrost JWT>",
  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
  "token_type": "Bearer",
  "expires_in": 900,
  "scope": "bifrost:api"
}
```

There is no `refresh_token`. Error responses use RFC 6749 / RFC 8693 codes
and the same cache header:

| Situation | `error` |
| --- | --- |
| Missing or malformed parameters, unsupported `subject_token_type`, `actor_token` present | `invalid_request` |
| Signature, issuer, audience, time, conditions, disabled or ambiguous rules, unknown identity | `invalid_grant` |
| `scope` other than `bifrost:api` | `invalid_scope` |
| `requested_token_type` other than an access token | `invalid_target` |
| Any other `grant_type` | `unsupported_grant_type` (existing behavior) |
| More than 60 exchanges from one client IP in a minute | HTTP 429 |

`invalid_grant` uses one description for every token failure so a caller
cannot tell which check failed. Rate limiting uses the existing Redis
limiter. If Redis cannot be updated, the request fails and no token is issued.
The testing environment does not enforce the limit, matching the other auth
limiters.

## Replay

Subject tokens are not single-use. A still-valid subject token can be
exchanged again. GitHub OIDC tokens and Azure managed-identity tokens are
already short-lived, audience-pinned, and signed by the issuer. A replay
cache would have to fail closed when its store is down; that store is not
part of this increment.

The issued BiFrost token is an ordinary bearer token until it expires or the
authority epoch changes. Replaying it inside that window is bearer-token
behavior, not a second exchange. Logout and human session revocation do not
apply, because there is no session.

## Audit

The token endpoint has no authenticated user. Audit rows use the anonymous
request actor and record the client IP.

- `auth.workload_exchange.success` — rule id, identity id, provider, verified
  issuer, verified `sub`, lifetime.
- `auth.workload_exchange.failed` — a reason code and, when the JWT can be
  decoded without trusting it, the unverified issuer and subject. The reason
  is for administrators. The HTTP body stays generic.
- `settings.workload_trust.create`, `.update`, and `.delete` — the acting
  administrator and the rule id. Pinned public keys may be stored on the rule.
  The subject token and the issued access token are never written to the audit
  payload or to logs.

## Client helper

Provider-specific acquisition stays in the CLI. `bifrost login --workload
github|azure --audience <audience>` reads the provider's own environment,
requests a token for that audience, and posts it to `/token`. It stores the
returned access token with an empty refresh token and the real expiry.

`BIFROST_API_URL` plus `BIFROST_ACCESS_TOKEN`, without
`BIFROST_REFRESH_TOKEN`, is enough for later commands. The CLI does not call
`/auth/refresh` when no refresh token is present. When the access token
expires, the workload runs the login again. On GitHub Actions, if `GITHUB_ENV`
is set, the helper appends `BIFROST_API_URL` and `BIFROST_ACCESS_TOKEN` so
later steps in the same job inherit them.

GitHub Actions reads `ACTIONS_ID_TOKEN_REQUEST_TOKEN` and
`ACTIONS_ID_TOKEN_REQUEST_URL` (the job needs `id-token: write`) and requests
the configured audience. Azure uses `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`
when both are set, and otherwise the instance metadata service at
`169.254.169.254` with the audience as the `resource` parameter. The helper
does not validate the provider token. The BiFrost token endpoint does.

## Threats

| Threat | Control |
| --- | --- |
| Stolen subject token replayed at this instance | Audience must be one configured for this rule. Another system's token does not match. Lifetime is the issuer's. |
| Stolen BiFrost token | Lifetime at most one hour, no refresh, epoch revocation on permission changes. |
| Issuer spoofing | Signature against the rule's keys, then exact `iss`. |
| Audience confusion | Exact `aud` match against the rule. The issued token uses BiFrost's audience, not the subject's. |
| Algorithm confusion | Asymmetric allowlist. `none` and HMAC rejected. Private JWK material rejected. |
| JWKS fetch SSRF | Canonical URL per profile, HTTPS only, no redirects, no proxy env, public addresses only, size and time caps, or pinned keys and no fetch. |
| Claim-condition footguns | Exact match only. No wildcards. `aud` and `iss` are not conditions. GitHub `sub` must agree with `repository`. Azure `tid` must agree with the issuer. |
| Over-broad subject matching | GitHub requires `repository` and `sub`. Azure requires `tid` and `oid`. Ambiguous rules fail closed. |
| Clock skew | 60 seconds on the subject token's time claims. BiFrost-issued tokens keep the existing access-token clock rules. |
| Token passthrough | The response access token is always minted here. The subject token is discarded. |
| Human MFA bypass | Identities cannot use password, device, or SSO login. The workload token is not a human session. |
| Logging secrets | Tokens are not logged. Audit stores claim names and reason codes. |

## Open questions

These are decisions for Thomas and upstream, not extra features in this change:

- Whether a subject-token `jti` should be single-use, and in which fail-closed store.
- Whether `appid` should be mandatory on Azure rules.
- Whether `bifrost:api` should later split into scopes that reduce an identity's permissions. Today the scope is a marker and the identity's roles are the authority.
- Whether a third, issuer-generic profile is wanted. Adding one is a new profile, not a schema change.
- Whether 60 exchanges per minute per IP is the right budget for a large shared NAT.
- Whether a workload token should keep working on MCP. This increment accepts it as the identity on the API and on MCP token verification.
