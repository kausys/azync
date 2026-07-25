# kernel (internal)

Import: `github.com/kausys/azync/workflow/kernel`

Pure in-memory workflow history engine: event log, command cursor, replay/park decisions. **No SQL, no driver, no network.**

## Why it exists

Separates deterministic replay math from persistence and worker orchestration:

- `workflow` loads history from `WorkflowStore`, runs handlers, schedules Operations/timers/signals.
- `kernel` only answers: given this history + next command, replay or park?

That keeps the hard nondeterminism rules unit-testable without Postgres.

## Who should import it

- Prefer `workflow` for applications.
- Import `kernel` only for tools, conformance helpers, or tests of the pure engine.

Parent package notes: [../README.md](../README.md) · User guide: [../../workflow.md](../../workflow.md).
