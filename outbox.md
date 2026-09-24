# Using the outbox

Commit a write to your own tables and the enqueue, publish or DAG it causes **together**, even when azync's tables live in another database — or when your tables live in many, one per tenant.

Package: [`driver/azyncpgx`](driver/azyncpgx/) · Example: [`examples/outbox`](https://github.com/kausys/azync/tree/main/examples/outbox)

## When you need it

A transaction lives in one database. When azync shares your database, enlist in it directly — `TxProducer`, `TxPublisher`, `TxRunner` ([queue.md](queue.md#transactional-outbox)). When it does not, a direct write cannot be atomic with yours: a commit followed by a publish can lose the event, and a publish followed by a rollback announces something that never happened.

The outbox is a table **beside your tables**. The write is captured there, in your transaction, and forwarded to azync afterwards.

```
your database                              azync's database
─────────────                              ────────────────
BEGIN
  INSERT INTO orders …
  capture event  ──▶ azync_outbox
COMMIT                                     
                     Drain ──────────────▶ ledger + deliveries / job / DAG
                     (removes what it forwarded)
```

## Set up

An `Outbox` holds configuration, never a connection. The same value serves every database it is migrated into.

```go
outbox, err := azyncpgx.NewOutbox(azyncpgx.WithOutboxSchema("app")) // lowercase identifier; empty = search_path

// once per database that holds one — with its own history and lock,
// independent of the store's Migrate
err = outbox.Migrate(ctx, appDB) // *sql.DB
```

It creates `azync_outbox` and its history `azync_outbox_migrations`. None of azync's other tables are created there.

## Capture

The runtimes build what is written; the outbox decides where it goes. `TxPublisherVia`, `TxProducerVia` and `TxRunnerVia` take its capture side, and the transaction type is inferred:

```go
publisher, err := events.TxPublisherVia(outbox.SQLTx()) // *sql.Tx
// outbox.TxStore() for pgx.Tx

tx, err := appDB.BeginTx(ctx, nil)
// your writes on tx …
_, err = publisher.PublishTx(ctx, tx, OrderPlaced{ID: id})
err = tx.Commit()
```

What is stored is exactly what the runtime built — id, payload, metadata, trace context, the compiled DAG — so nothing is rebuilt when it is forwarded, and the consumer's span still hangs from the request that made the write.

Because fan-out and deduplication happen when a row is **forwarded**, a capture cannot report them: `PublishTx` reports no deliveries, and `EnqueueTx` / `RunTx` never report `Deduplicated`. Idempotency keys are still honoured — against azync's state at forwarding time.

## Forward

`Drain` hands up to `limit` committed rows to a store, oldest first, and removes each one it forwarded:

```go
res, err := outbox.Drain(ctx, appDB, core.Store(), 100)
// res.Forwarded, res.Quarantined
```

Call it until a batch comes back short. The usual trigger is a signal sent **after** each commit — a queue job coalesced per outbox, so many commits make one pending drain and none is lost:

```go
// after tx.Commit()
_, err = jobs.Producer().Enqueue(ctx, DrainOutbox{Name: "app"}, queue.CoalesceKey("app"))
```

A signal that is lost — the process died between the commit and the enqueue — only delays the rows until the next one. Keep a periodic drain (a cron) as the backstop for that case.

## Guarantees

| | |
|--|--|
| Atomicity | A row exists only if your transaction committed. |
| Delivery | At-least-once forwarding, never a duplicate: the store refuses an id it already has (`driver.ErrAlreadyExists`) and `Drain` counts that as done. A crash between forwarding and removing costs a retry, not a second event. |
| Concurrency | Rows are claimed `FOR UPDATE SKIP LOCKED`: any number of drains can run against one database, and each row is handed over once. Two overlapping drains split the rows; one may forward nothing. |
| Failure | A transient error stops the batch: rows already forwarded are removed, the rest wait, the error is returned. |
| Poison | A row that can never be forwarded — undecodable params, an unknown format or kind — is **quarantined** (`quarantined_at`, `last_error`) and the batch goes on. |
| Order | Oldest first within a batch, but azync's delivery is unordered by contract: consumers deduplicate by `(EventID, Subscriber)` as always. |
| Horizon | Deduplication lasts as long as the store keeps what a row was forwarded as. A row held back longer than the ledger's retention could be forwarded again. |

## Operations

Quarantined rows stay in the table, out of every drain:

```sql
SELECT id, kind, topic, created_at, last_error
FROM app.azync_outbox WHERE quarantined_at IS NOT NULL;

-- after fixing the cause, put one back in line
UPDATE app.azync_outbox SET quarantined_at = NULL, last_error = NULL WHERE id = '…';
```

The backlog of one database is `SELECT count(*), min(created_at) FROM app.azync_outbox WHERE quarantined_at IS NULL` — the age of the oldest row is the number worth alerting on.

## See also

[queue.md](queue.md) · [event.md](event.md) · [dag.md](dag.md) · [README](README.md)
