# Operations

Check `docker compose ps`, `/health/ready`, `/queue`, `/audit`, and `/reconcile`. Inspect `docker compose logs --tail=100 api relay publisher consumer`. Match a transfer/event UUID in JSON logs and Jaeger spans. API, publisher and relay propagate trace context where applicable; consumer traces correlate by event ID.

**Accepted request does not finish:** inspect pending outbox and delivery backlog. Publisher absent means work remains in outbox; relay unavailable means capped retries. Consumer absent means relay rows remain pending. A lease held by a crashed process becomes eligible after its ten-second expiry. Do not erase pending rows to clear the symptom.

**Publisher crash after receipt:** the relay may already contain the event while `published_at` is null. Restart the normal publisher; duplicates are expected. Verify one journal/inbox per transfer and a balanced reconciliation report. `make recovery` demonstrates the real process exit and recovery.

**Dead letter:** inspect `/queue` and corresponding audit records. Permanent errors retain their validation reason. Transient failures reach `attempt_limit` after eight processing attempts. Repair the dependency/cause first, then run `docker compose run --rm --no-deps consumer retry DELIVERY_ID`. The explicit retry is audited; unknown or forged requests remain invalid.

**Reconciliation drift:** retain the report, capture related journals and transfer outcomes, then investigate. Do not directly edit posted entries or overwrite balances to hide the mismatch. The synthetic unsafe concurrency exercise intentionally demonstrates cache-to-ledger drift in an isolated test schema.

**Retention and shutdown:** PostgreSQL data persists in the named volume; no automatic retention is advertised. Runtime JSON logs rotate at 10 MB with three files per application service. Jaeger and Grafana data are ephemeral. Shutdown cancels processing; leases and durable records allow recovery. `make down` stops services while retaining database data.

The fault harness stops only this named Compose project's publisher and consumer and restores them on exit. It requires a local Unix-socket Docker context and a quiescent synthetic workload at fault setup. The harness checks the particular transfer's immutable entries so later unrelated valid transfers do not invalidate a whole-account snapshot assumption.
