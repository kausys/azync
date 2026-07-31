# Migrations

Applied via [goose](https://github.com/pressly/goose) (`../migrate.go`), gated
by a session advisory lock (`goose.WithSessionLocker`) so `Migrate` is safe to
call concurrently from multiple instances.

## Released migrations are frozen

Goose tracks applied migrations by version number only — it does not
checksum file contents. Editing an already-released migration file is
**silent, permanent schema drift**: an environment that already ran version
`N` never re-runs it, so it keeps whatever DDL was in the file *at the time
it migrated*, while a fresh install on the new code runs the *edited* `N`
and ends up with a different schema — both recorded as "version N applied"
with no way to tell them apart.

Once a migration file has shipped in a release, it is immutable. Every
schema change — including fixing a mistake in an earlier migration — is a
new, higher-numbered file. (`00005_drop_tenant_trace_columns.sql` is exactly
this: it re-expresses a change that was mistakenly made by editing `00001`
and `00002` in place.)

## Writing a migration that touches `azync_jobs`

`azync_jobs` is the shared, potentially large, hot table every runtime reads
and writes continuously. Two DDL patterns silently take a full-table,
`ACCESS EXCLUSIVE` lock for as long as it takes Postgres to scan or rewrite
the table — on a busy production table that is a full outage for every
runtime, not just a slow migration:

1. **Adding or changing a `CHECK` constraint** with a plain
   `ADD CONSTRAINT ... CHECK (...)` validates the constraint against every
   existing row while holding the exclusive lock. Split it in two: add the
   constraint `NOT VALID` (an `ACCESS EXCLUSIVE` lock that only touches the
   catalog, no scan) and validate it separately (a `SHARE UPDATE EXCLUSIVE`
   lock that scans concurrently with reads and writes).

   ```sql
   -- +goose Up
   ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
   ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
       CHECK (state IN (...)) NOT VALID;
   ALTER TABLE azync_jobs VALIDATE CONSTRAINT azync_jobs_state_check;
   ```

2. **Building an index** with a plain `CREATE INDEX` takes the same
   `ACCESS EXCLUSIVE` lock for the duration of the build. Use
   `CREATE INDEX CONCURRENTLY`, which requires running outside goose's
   per-file transaction and being idempotent on its own (a `NO TRANSACTION`
   migration can partially apply if it fails partway through):

   ```sql
   -- +goose NO TRANSACTION
   -- +goose Up
   CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_jobs_some_new_idx
       ON azync_jobs (...);

   -- +goose Down
   DROP INDEX CONCURRENTLY IF EXISTS azync_jobs_some_new_idx;
   ```

`00001`–`00004` predate this policy and were written against an empty table
at install time; they are not rewritten (see above) and remain a hazard only
for upgrading an already-populated `azync_jobs` — a risk that is now closed
for every migration from `00005` on.

## Triggers

`00011` introduces the first triggers: statement-level change-hint emitters
on `azync_jobs`, `azync_dags` and `azync_events` backing the
`driver.ChangeNotifier` capability (see that file's header comment for the
full rationale). Two rules they establish:

- `CREATE OR REPLACE TRIGGER` is PG14+ and this driver supports PG13, so
  trigger idempotency is `DROP TRIGGER IF EXISTS` + `CREATE TRIGGER`
  (functions use `CREATE OR REPLACE FUNCTION`).
- Any `$$ ... $$` body **must** be wrapped in `-- +goose StatementBegin` /
  `-- +goose StatementEnd`, or goose's semicolon splitter shreds it.
