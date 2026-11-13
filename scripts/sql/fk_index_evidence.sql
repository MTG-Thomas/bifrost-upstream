-- Foreign-key index evidence: read-only ranking of FK columns with no
-- leading-prefix index.
--
-- Read-only. No DDL, no writes, no statistics resets. Safe on the primary,
-- cheaper on a replica (per-instance counters, so a replica only shows reads
-- that replica served).
--
--   psql "$PROD_DSN" -X -v ON_ERROR_STOP=1 -f scripts/sql/fk_index_evidence.sql
--
-- Output columns: table_name, column_name, seq_scan, idx_scan, insert_rate,
-- index_bytes, calls, mean_ms, plus est_rows and fk_name for triage.
--
--   seq_scan / idx_scan  cumulative since the counters were last reset
--   insert_rate          (n_tup_ins + n_tup_upd + n_tup_del) per day over that
--                        same window; a scan per write is the FK-trigger shape
--   index_bytes          current total index footprint of the child table, i.e.
--                        what another index is added to (raw bytes)
--   calls / mean_ms      from pg_stat_statements, matched by table + column
--                        name in the normalized statement text (heuristic);
--                        NULL when the extension is absent
--
-- Take two snapshots and subtract for a window; see
-- docs/analysis/fk-index-evidence.md.
--
-- Composite foreign keys are excluded: this checks single-column FKs, which is
-- what the splinter finding reports. A composite FK's leading column is a
-- separate question.

\set ON_ERROR_STOP on
\pset pager off

-- The extension's view can live outside the search path; set that schema here
-- if needed (for example: SET search_path = public, extensions;).
SELECT (to_regclass('pg_stat_statements') IS NOT NULL) AS has_pss \gset

\if :has_pss
\set pss_join 'LEFT JOIN LATERAL ( SELECT sum(s.calls)::bigint AS calls, round((sum(s.total_exec_time) / NULLIF(sum(s.calls), 0))::numeric, 2) AS mean_ms FROM pg_stat_statements s WHERE s.query LIKE ''%'' || fk.table_name || ''%'' AND s.query LIKE ''%'' || fk.column_name || ''%'' ) pss ON true'
\set pss_cols ', pss.calls, pss.mean_ms'
\else
\set pss_join ''
\set pss_cols ', NULL::bigint AS calls, NULL::numeric AS mean_ms'
\endif

WITH fk AS (
    SELECT con.conname,
           c.relname AS table_name,
           a.attname AS column_name,
           con.conrelid
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a
      ON a.attrelid = con.conrelid
     AND a.attnum = con.conkey[1]
    WHERE con.contype = 'f'
      AND n.nspname = 'public'
      AND array_length(con.conkey, 1) = 1
      AND NOT EXISTS (
          SELECT 1
          FROM pg_index i
          WHERE i.indrelid = con.conrelid
            AND i.indisvalid
            AND i.indpred IS NULL          -- partial indexes cannot serve RI
            AND i.indkey[0] = con.conkey[1] -- leading index column
      )
),
stat AS (
    SELECT relid, seq_scan, idx_scan,
           coalesce(n_tup_ins, 0) AS n_tup_ins,
           coalesce(n_tup_upd, 0) AS n_tup_upd,
           coalesce(n_tup_del, 0) AS n_tup_del,
           n_live_tup
    FROM pg_stat_user_tables
    WHERE schemaname = 'public'
),
win AS (
    SELECT greatest(
               extract(epoch FROM now() - pg_stat_get_db_stat_reset_time(d.oid))
               / 86400.0,
               1.0
           ) AS days
    FROM pg_database d
    WHERE d.datname = current_database()
)
SELECT fk.table_name,
       fk.column_name,
       stat.seq_scan,
       stat.idx_scan,
       round((stat.n_tup_ins + stat.n_tup_upd + stat.n_tup_del) / win.days, 1)
           AS insert_rate,
       pg_indexes_size(fk.conrelid) AS index_bytes
       :pss_cols,
       stat.n_live_tup AS est_rows,
       fk.conname AS fk_name
FROM fk
LEFT JOIN stat ON stat.relid = fk.conrelid
CROSS JOIN win
:pss_join
ORDER BY stat.seq_scan DESC NULLS LAST,
         stat.n_live_tup DESC NULLS LAST;
