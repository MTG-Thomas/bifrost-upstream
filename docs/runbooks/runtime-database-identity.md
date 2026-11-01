# Runtime database session identity

The existing detailed health check samples `session_user`, `current_user` and
`current_database()` using the same SQLAlchemy session and statement as its
connectivity check. It does not open a diagnostic connection or add an endpoint.
Identity values are never included in health HTTP responses.

The first successful observation marks the pooled client connection's `info`.
A later successful check on that same cached DBAPI connection can emit one
sanitized `Runtime database session identity` log per API process. A connection
replacement clears its pool info, so its first sample does not count as reuse.
This proves the sampled client connection was observed previously, not that every
pool/process uses the same role. A pooler can change the server backend behind
one cached client connection. No historical server-session identity is inferred.
`session_user` and `current_user` are both retained because SET ROLE can make
them differ. Only bounded safe identity strings are logged; unexpected fields or
unsafe values produce a constant warning without the values.

`/health/ready` keeps its existing startup-result cache. Use the existing
`/health/detailed` diagnostic path when a fresh dependency observation is needed;
its response shape remains unchanged. Record the exact deployed source/image,
API process/instance and the identity log time. A process can legitimately have
no sample yet if no checked connection has been observed twice. Do not create
connections, recycle a pool, restart processes, or loop probes until a desired
identity appears and call that cached-session proof.

This source change is not production activation authorization. For the existing
BiFrost production and canary API sidecars, publishing a qualified candidate and
replacing the running image requires the normal protected deployment and
maintenance/recovery decision. Existing pools do not gain instrumentation merely
because this patch is merged. A deployment replaces old processes: observations
then describe connections in the new running application, not the pre-deployment
pool. No old cached-session uncertainty is accepted on the operator's behalf.
DMARC's .NET/Npgsql path needs its own reviewed implementation and evidence.
Credential rotation, database changes, grants, and ARM status-read proof remain
separate gates. The observer does not prove any of them.
