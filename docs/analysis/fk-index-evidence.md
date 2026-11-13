# Foreign Key Index Evidence

Date: 2026-11-13

Research only. No production database access, no index migration, no query
statistics, no PR. Base: `upstream/main` at `7be95b942`.

Production Postgres (PG16) currently reports **33 foreign keys with no
leading-prefix covering index**. `pg_stat_statements` is not installed, so this
document ranks the candidates from **code shape and referential-integrity
semantics**, not from measured volume. Section
[Deciding with a 7-day window](#deciding-with-a-7-day-window) is how each
`needs query stats` call gets settled.

## Two jobs a foreign-key index does

An index whose leading column is the FK column is used for two unrelated things,
and this ranking counts both:

1. **Child-side predicate.** The column appears in `WHERE`, `JOIN` or
   `ORDER BY`, so the planner can seek instead of scanning the child table.
2. **Parent-side `DELETE`/`UPDATE`.** Every parent row removed fires the RI
   trigger, which must find matching child rows. Without an index that is one
   sequential scan of the child table *per parent row* — the failure mode that
   turns a batch delete into a quadratic job.

Rows where neither applies are `skip`, even though the splinter finding is
technically correct for them.

The FK only needs the **leading** column. Where the code's real shape is compound
(`conversation_id` + `created_at DESC`, `provider_id` + `organization_id`), a
composite index satisfies the FK and the query; that refinement is noted per row.

## Filter-density context

API-wide column-name occurrence counts supplied with the task:
`organization_id` 249, `user_id` 78, `role_id` 30, `conversation_id` 28,
`form_id` 18, `created_by` 0. These count occurrences of a name across tables,
not predicates on one FK and not execution frequency, so they are context only.
Per-FK evidence below is what drives the tier.

## (a) Add now

| FK | table.column | Working predicates and RI paths (file:line) | Why it is `add now` |
| --- | --- | --- | --- |
| `executions_form_id_fkey` | `executions.form_id` | Purge unlinks explicitly: `UPDATE executions SET form_id = NULL WHERE form_id = :id` ([routers/forms.py:1204](api/src/routers/forms.py#L1204)). Repo-file deletion does not: `delete(Form)` ([file_storage/indexers/form.py:306](api/src/services/file_storage/indexers/form.py#L306)) fires the FK's `SET NULL` per row. The column is otherwise a write stamp ([repositories/executions.py:75](api/src/repositories/executions.py#L75)) and a serialized field ([routers/executions.py:364](api/src/routers/executions.py#L364)). | `executions` is the largest accumulating table in the schema, and the app's own purge path already runs an unindexed `form_id` predicate over all of it. Form purge is a normal product action (`DELETE /api/forms/{id}?purge=true`). |
| `agent_runs_conversation_id_fkey` | `agent_runs.conversation_id` | `_latest_run` filters and orders on it on every chat state load: [services/chat_runs.py:621](api/src/services/chat_runs.py#L621) (`ORDER BY created_at DESC, id DESC LIMIT 1`), reached from `get_chat_state` ([services/chat_runs.py:496](api/src/services/chat_runs.py#L496)). Ownership-checked cancel joins on it: [services/chat_runs.py:522](api/src/services/chat_runs.py#L522). | Per-page-load predicate over a table that grows with every agent run. A composite `(conversation_id, created_at DESC)` satisfies the FK and the ordered lookup; the FK alone needs only `conversation_id`. |
| `oauth_tokens_provider_id_fkey` | `oauth_tokens.provider_id` | Credential resolution filters it on the integration-tool path: [repositories/oauth.py:186](api/src/repositories/oauth.py#L186), [:227](api/src/repositories/oauth.py#L227), [:380](api/src/repositories/oauth.py#L380), [:421](api/src/repositories/oauth.py#L421), [:439](api/src/repositories/oauth.py#L439); [services/oauth_storage.py:341](api/src/services/oauth_storage.py#L341); [routers/integrations.py:245](api/src/routers/integrations.py#L245). Bulk deletes filter it too: [routers/integrations.py:467](api/src/routers/integrations.py#L467); [services/oauth_storage.py:287](api/src/services/oauth_storage.py#L287). | Every tool call that resolves an integration credential starts with `provider_id` equality, and disconnect deletes tokens by provider. Composite `(provider_id, organization_id, user_id)` matches both shapes. |
| `agent_runs_event_delivery_id_fkey` | `agent_runs.event_delivery_id` | Nightly run/event retention deletes events in batches ([services/run_retention/deleter.py:216](api/src/services/run_retention/deleter.py#L216), `DELETE FROM events ...` at [:235](api/src/services/run_retention/deleter.py#L235)); deliveries cascade ([models/orm/events.py:410](api/src/models/orm/events.py#L410)), and each deleted delivery fires `SET NULL` into `agent_runs` ([models/orm/agent_runs.py:28](api/src/models/orm/agent_runs.py#L28)). Writes: [services/execution/agent_run_service.py:137](api/src/services/execution/agent_run_service.py#L137), [services/events/processor.py:990](api/src/services/events/processor.py#L990). | Retention runs daily with a 30-day default window against a table that only grows. Without the index, every deleted delivery costs a full scan of `agent_runs`, so one night's batch multiplies into thousands of scans. |

## (b) Needs query stats

Real predicates or real RI paths exist, but either the child table is small, or
the path is admin/batch rather than per-request. Decide from
[the 7-day window](#deciding-with-a-7-day-window) using `seq_scan` delta, table
size and `calls`.

| FK | table.column | Working predicates and RI paths (file:line) | Call |
| --- | --- | --- | --- |
| `agent_tools_workflow_id_fkey` | `agent_tools.workflow_id` | Workflow analysis and orphan remap: [services/workflow_orphan.py:513](api/src/services/workflow_orphan.py#L513), [:524](api/src/services/workflow_orphan.py#L524), [:597](api/src/services/workflow_orphan.py#L597); dependency walker [services/solutions/dependency_walker.py:485](api/src/services/solutions/dependency_walker.py#L485), [:500](api/src/services/solutions/dependency_walker.py#L500); deactivation reference scan [file_storage/deactivation.py:132](api/src/services/file_storage/deactivation.py#L132); graph [dependency_graph.py:426](api/src/services/dependency_graph.py#L426), [:500](api/src/services/dependency_graph.py#L500), [:612](api/src/services/dependency_graph.py#L612); join [execution/agent_workflow_tools.py:77](api/src/services/execution/agent_workflow_tools.py#L77). Workflows are deactivated, never hard-deleted, so there is no parent-delete driver. | Add if `agent_tools` is non-trivial and workflow delete/analysis `calls` are frequent. The table is `agents × tools`, so it is usually modest. |
| `form_fields_data_provider_id_fkey` | `form_fields.data_provider_id` | Workflow delete/existence scan [file_storage/deactivation.py:103](api/src/services/file_storage/deactivation.py#L103); orphan cleanup [services/workflow_orphan.py:467](api/src/services/workflow_orphan.py#L467), [:571](api/src/services/workflow_orphan.py#L571); dependency graph [dependency_graph.py:405](api/src/services/dependency_graph.py#L405), [:596](api/src/services/dependency_graph.py#L596). Note [routers/workflows.py:384](api/src/routers/workflows.py#L384) is `form_id`-led, not this column. | Add if workflow deletion/deactivation is frequent and `form_fields` has grown; it is a per-form field table, so scan cost is usually small. |
| `mcp_connections_service_oauth_token_id_fkey` | `mcp_connections.service_oauth_token_id` | Refresh scheduler subquery every 15 minutes: [jobs/schedulers/oauth_token_refresh.py:105](api/src/jobs/schedulers/oauth_token_refresh.py#L105). RI `SET NULL` fires on every token delete, including bulk provider disconnect ([routers/integrations.py:467](api/src/routers/integrations.py#L467)). | Add if integration disconnects delete many tokens, or `mcp_connections` grows. The scheduled read is a small-table scan; the RI path is the real cost. |
| `user_roles_role_id_fkey` | `user_roles.role_id` | Role-filtered membership delete [services/user_role_assignments.py:651](api/src/services/user_role_assignments.py#L651) (also constrained by `user_id`, so PK-led). Role-name joins [services/authorization/tokens.py:586](api/src/services/authorization/tokens.py#L586), [:690](api/src/services/authorization/tokens.py#L690); [services/identities.py:142](api/src/services/identities.py#L142); [routers/websocket.py:326](api/src/routers/websocket.py#L326). Parent delete: `DELETE FROM roles` relies on `ON DELETE CASCADE` ([shared/sdk_roles.py:536](api/shared/sdk_roles.py#L536)). | Add if `DELETE FROM roles` is a real operation in the window and `user_roles` is large; every role delete otherwise scans `user_roles`. Per-request joins are already `user_id`-led by the PK. |
| `agent_roles_role_id_fkey` | `agent_roles.role_id` | Projection-only in code ([services/workflow_role_service.py:173](api/src/services/workflow_role_service.py#L173), [solutions/workspace_bundle_plan.py:595](api/src/services/solutions/workspace_bundle_plan.py#L595)); no role predicate. Parent delete: role cascade ([shared/sdk_roles.py:536](api/shared/sdk_roles.py#L536)) and agent delete cascade ([routers/agents.py:798](api/src/routers/agents.py#L798) removes by `agent_id`, covered by the PK). | Only role deletion benefits. Cheap, small table — measure before adding. |
| `form_roles_role_id_fkey` | `form_roles.role_id` | Projection-only ([services/workflow_role_service.py:166](api/src/services/workflow_role_service.py#L166), [solutions/workspace_bundle_plan.py:590](api/src/services/solutions/workspace_bundle_plan.py#L590)). Parent delete: role cascade. | Same as `agent_roles.role_id`; table is tiny. |
| `agent_delegations_child_agent_id_fkey` | `agent_delegations.child_agent_id` | Joins on the child edge: [services/agent_runtime/composition.py:183](api/src/services/agent_runtime/composition.py#L183), [:663](api/src/services/agent_runtime/composition.py#L663); [execution/autonomous_agent_executor.py:1332](api/src/services/execution/autonomous_agent_executor.py#L1332); bundle plan [solutions/workspace_bundle_plan.py:605](api/src/services/solutions/workspace_bundle_plan.py#L605). Those joins are driven by `parent_agent_id` (already indexed) or `Agent.id`, so the child index mostly helps `DELETE FROM agents` ([routers/agents.py:798](api/src/routers/agents.py#L798)). | Add if agent hard-deletes happen and the delegation table is not tiny. Low risk either way. |
| `oauth_tokens_user_id_fkey` | `oauth_tokens.user_id` | Per-user token upsert [services/oauth_storage.py:342](api/src/services/oauth_storage.py#L342); org-level token reads add `user_id IS NULL` after `provider_id` ([repositories/oauth.py:381](api/src/repositories/oauth.py#L381), [:423](api/src/repositories/oauth.py#L423), [:441](api/src/repositories/oauth.py#L441)). Parent delete: user delete `SET NULL`. | Covered by the same `provider_id` index once added (second column). Add separately only if `user_id`-first lookups appear in the window. |
| `oauth_tokens_organization_id_fkey` | `oauth_tokens.organization_id` | Org-scoped token selection [repositories/oauth.py:230](api/src/repositories/oauth.py#L230), [:422](api/src/repositories/oauth.py#L422), [:440](api/src/repositories/oauth.py#L440); [routers/integrations.py:248](api/src/routers/integrations.py#L248); [services/oauth_provider.py:786](api/src/services/oauth_provider.py#L786). Always after `provider_id`. | Redundant once `(provider_id, organization_id, user_id)` exists; otherwise measure. |
| `ai_usage_user_id_fkey` | `ai_usage.user_id` | No predicate anywhere in `api/src` — write-only stamp ([models/orm/ai_usage.py:131](api/src/models/orm/ai_usage.py#L131)). Only user deletion (`SET NULL`) touches it. | Add only if `n_tup_del` on `users` is non-zero in the window **and** `ai_usage` is large; otherwise the one-off scan does not justify permanent write cost on the hottest insert table. |
| `agent_runs_verdict_set_by_fkey` | `agent_runs.verdict_set_by` | Write-only ([routers/agent_runs.py:700](api/src/routers/agent_runs.py#L700), [:750](api/src/routers/agent_runs.py#L750); [execution/tuning_service.py:400](api/src/services/execution/tuning_service.py#L400)). Only user deletion touches it. | Same test as `ai_usage.user_id`: user-delete volume decides. |
| `agent_run_verdict_history_changed_by_fkey` | `agent_run_verdict_history.changed_by` | Write-only ([routers/agent_runs.py:707](api/src/routers/agent_runs.py#L707), [:757](api/src/routers/agent_runs.py#L757)). | Only user deletion. Table grows with agent runs, so a large `users` delete could be expensive; measure before adding. |
| `agent_prompt_history_changed_by_fkey` | `agent_prompt_history.changed_by` | Write-only ([execution/tuning_service.py:386](api/src/services/execution/tuning_service.py#L386)). | Only user deletion; table grows with prompt edits. Measure. |
| `knowledge_store_organization_id_fkey` | `knowledge_store.organization_id` | Org reach predicate on listings and reindex: [routers/knowledge_sources.py:90](api/src/routers/knowledge_sources.py#L90), [:92](api/src/routers/knowledge_sources.py#L92); [services/embeddings/reindex.py:113](api/src/services/embeddings/reindex.py#L113). | `knowledge_store` is one row per source, not per document. Add only if `n_live_tup` is large enough that the page-load scan shows up. |
| `solution_workflow_permission_requests_workflow_id_fkey` | `solution_workflow_permission_requests.workflow_id` | Filtered by workflow set during analysis/apply: [services/workflow_permissions.py:125](api/src/services/workflow_permissions.py#L125), [:254](api/src/services/workflow_permissions.py#L254), ordered at [:233](api/src/services/workflow_permissions.py#L233). | Admin/batch path over a small table; add only if conversion runs show up hot. |

## (c) Skip

No usable predicate and no reachable parent delete, or the table is
structurally tiny (hundreds to low thousands of rows), so a sequential scan is
already the cheapest plan.

| FK | table.column | Evidence (file:line) | Why skip |
| --- | --- | --- | --- |
| `ai_usage_message_id_fkey` | `ai_usage.message_id` | Only use is `count(distinct AIUsage.message_id)` inside a conversation/date aggregate ([routers/usage_reports.py:263](api/src/routers/usage_reports.py#L263)). Messages are never hard-deleted (conversation delete is a soft delete, [routers/chat.py:314](api/src/routers/chat.py#L314)). | The aggregate is driven by `conversation_id` + date range; an index on `message_id` would not be chosen and there is no parent delete. |
| `agent_mcp_connections_granted_by_fkey` | `agent_mcp_connections.granted_by` | Write-only grant audit ([routers/agents.py:443](api/src/routers/agents.py#L443), [:751](api/src/routers/agents.py#L751); [repositories/agents.py:286](api/src/repositories/agents.py#L286)). | Tiny table, audit-only column, user delete is rare. |
| `app_embed_secrets_created_by_fkey` | `app_embed_secrets.created_by` | Write at [routers/app_embed_secrets.py:68](api/src/routers/app_embed_secrets.py#L68); all reads filter `application_id` ([routers/app_embed_secrets.py:98](api/src/routers/app_embed_secrets.py#L98), [:130](api/src/routers/app_embed_secrets.py#L130)). | Secret rows per app are few and never queried by creator. |
| `form_embed_secrets_created_by_fkey` | `form_embed_secrets.created_by` | All reads filter `form_id` ([routers/form_embed_secrets.py:110](api/src/routers/form_embed_secrets.py#L110), [:142](api/src/routers/form_embed_secrets.py#L142)). | Same shape as app embed secrets. |
| `form_publications_created_by_fkey` | `form_publications.created_by` | Public lookup is by `public_key` ([routers/embed.py:75](api/src/routers/embed.py#L75)); `created_by` is written once. | Never filtered; tiny table. |
| `knowledge_store_created_by_fkey` | `knowledge_store.created_by` | Written at [routers/knowledge_sources.py:399](api/src/routers/knowledge_sources.py#L399), copied at [:528](api/src/routers/knowledge_sources.py#L528); no `created_by` predicate. | One row per source; never filtered by creator. |
| `configs_organization_id_fkey` | `configs.organization_id` | Every predicate leads with a more selective column: [repositories/integrations.py:470](api/src/repositories/integrations.py#L470) (`integration_id`, `key`), [solutions/setup_status.py:44](api/src/services/solutions/setup_status.py#L44), [solutions/zip_install.py:981](api/src/services/solutions/zip_install.py#L981). | `integration_id` is already indexed and leads; org is a residual filter on a small table. |
| `user_role_boundaries_organization_id_fkey` | `user_role_boundaries.organization_id` | Projection-only ([services/user_role_assignments.py:328](api/src/services/user_role_assignments.py#L328), [services/identities.py:135](api/src/services/identities.py#L135), [services/authorization/context.py:134](api/src/services/authorization/context.py#L134)). | No org predicate exists; rows are per user-role boundary. |
| `users_base_role_id_fkey` | `users.base_role_id` | Read by user ([services/authorization/context.py:125](api/src/services/authorization/context.py#L125), [enforce.py:376](api/src/services/authorization/enforce.py#L376)); the only `base_role_id` predicate is the role-delete guard count ([shared/sdk_roles.py:523](api/shared/sdk_roles.py#L523)). | `users` is small and the predicate runs only on role deletion. |
| `mcp_servers_oauth_provider_id_fkey` | `mcp_servers.oauth_provider_id` | Read through the server row ([services/mcp_client/auth_resolution.py:156](api/src/services/mcp_client/auth_resolution.py#L156), [routers/mcp_servers.py:120](api/src/routers/mcp_servers.py#L120), [routers/mcp_oauth_callback.py:129](api/src/routers/mcp_oauth_callback.py#L129)); RI `RESTRICT` check runs only on provider delete. | Provider deletes are rare and the server table is small. |
| `webhook_sources_integration_id_fkey` | `webhook_sources.integration_id` | No predicate; integration delete is a soft delete that keeps the row ([routers/integrations.py:454](api/src/routers/integrations.py#L454)), so the `SET NULL` never fires. | No read path and no reachable parent delete. |
| `fk_solutions_git_source` | `solutions.git_source_id` | Constraint added `ON DELETE NO ACTION` ([alembic/versions/20261109_solution_git_source.py:13](api/alembic/versions/20261109_solution_git_source.py#L13)); value is read/forwarded ([services/solutions/git_sync.py:258](api/src/services/solutions/git_sync.py#L258), [jobs/schedulers/solution_update_check.py:45](api/src/jobs/schedulers/solution_update_check.py#L45)). | No predicate, no delete path (NO ACTION blocks it by design), and `system_configs` is small. |
| `workflow_conversion_reports_workflow_id_fkey` | `workflow_conversion_reports.workflow_id` | Never filtered; reports are read by `job_id` ([models/orm/workflow_conversions.py:65](api/src/models/orm/workflow_conversions.py#L65) declares it, `job_id` is indexed at [:90](api/src/models/orm/workflow_conversions.py#L90)). | No predicate, and workflows are deactivated rather than deleted, so the cascade never runs. |
| `schedules_created_by_fkey` | `schedules.created_by` | No `schedules` table exists in the current model layer: schedules are `event_sources` + `schedule_sources` ([models/orm/events.py:132](api/src/models/orm/events.py#L132)). | Legacy table left in production. Confirm with `to_regclass('public.schedules')` and row count; likely drop candidate, not index candidate. |

## Deciding with a 7-day window

`scripts/sql/fk_index_evidence.sql` is the read-only ranking query. It lists
every single-column FK in `public` without a valid, non-partial index whose
first column is the FK column, and joins `pg_stat_user_tables`,
`pg_indexes_size` and (when present) `pg_stat_statements`.

Counters are cumulative, so a "7-day window" means two snapshots and a
difference. Run against the primary (or a replica that receives the same
traffic; statistics are per instance, so a replica shows only its own reads):

```bash
# Day 0
psql "$PROD_DSN" -X -v ON_ERROR_STOP=1 \
  -f scripts/sql/fk_index_evidence.sql > fk_evidence_day0.txt
psql "$PROD_DSN" -X -c "\copy (SELECT relid::regclass::text AS table_name, \
  seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup \
  FROM pg_stat_user_tables WHERE schemaname = 'public') \
  TO 'fk_tables_day0.csv' CSV HEADER"

# Day 7: same two commands, day7 filenames, then subtract per table
#   seq_delta      = day7.seq_scan      - day0.seq_scan
#   write_delta    = day7.(ins+upd+del) - day0.(ins+upd+del)
```

Rules for reading the result:

- `calls` is `NULL` until `pg_stat_statements` exists. Installing it is a
  server-parameter plus `CREATE EXTENSION` change in the authorized database
  lane, not part of this research.
- Add the index when `seq_scan` delta is proportional to `write_delta`
  (a scan per write is the FK-trigger signature) or when the table's
  `n_live_tup` makes one scan expensive and the predicate runs per request.
- Keep `skip` when the table's row count stays in the low thousands: PostgreSQL
  is right to scan it, and the extra index only adds write cost.
- `pg_stat_reset()`, crash recovery and a major-version upgrade reset these
  counters; restart the window if any of those lands inside it.

## Rollout note (no migration in this branch)

One index per migration, `CONCURRENTLY`, following
[`api/alembic/versions/20260912_document_prefix_index.py`](api/alembic/versions/20260912_document_prefix_index.py):

```python
def upgrade() -> None:
    with op.get_context().autocommit_block():
        try:
            op.execute("SET lock_timeout = '5s'")
            op.execute("SET statement_timeout = '30min'")
            op.execute(
                "CREATE INDEX CONCURRENTLY ix_executions_form_id "
                "ON executions (form_id)"
            )
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        try:
            op.execute("SET lock_timeout = '5s'")
            op.execute("SET statement_timeout = '30min'")
            op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_executions_form_id")
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")
```

- One index per migration: a failed concurrent build leaves an invalid index and
  blocks nothing else.
- `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block;
  `autocommit_block()` is what makes it legal.
- A 5s `lock_timeout` bounds the lock wait and a 30min `statement_timeout`
  bounds the build; on timeout the migration fails rather than holding locks.
- After a failure, check `pg_index.indisvalid` and drop the invalid index before
  retrying; `DROP INDEX CONCURRENTLY IF EXISTS` is in `downgrade`.
- FK indexes are pure performance: they can ship before or after the code that
  benefits, unlike the access-path index in the reference migration.
- Verify with `EXPLAIN (ANALYZE, BUFFERS)` on the exact predicate from the
  evidence table before and after, on the deployment's own instance.
