# Engineering review

Author-conducted review of the local implementation, SQL boundaries, worker failure handling, test evidence, and documentation. This is not independent approval or a financial compliance assessment.

## Findings and resolutions

| Severity | Finding | Resolution |
| --- | --- | --- |
| High | Reconciliation could accept a balanced journal with the wrong registered transfer amount | A new regression first failed for the intended reason; reconciliation now independently checks account direction, exact amount, two-entry transfer shape, and duplicate journals. Before/after output is retained. |
| High | Receipt or replay without business idempotency permits duplicate accounting effects | Inbox checks and unique journal transfer identity; incident 001 demonstrates the unsafe isolated legacy path and corrected behavior. |
| High | Uncertain relay acknowledgement or publisher crash must not clear committed pending work | Transactional outbox, acknowledgement-after-durable-relay-receipt, expiring leases, and duplicate-safe consumers; both dropped-response and real process-exit evidence captured. |
| High | Concurrent stale absolute balance updates can overspend and drift from journals | Stable account-ID locking, funds/overflow checks, one posting transaction, database constraints, randomized concurrency, and incident 003. |
| Medium | Whole-account recovery snapshots were invalidated by an overlapping valid demo | Harness now verifies the particular transfer's immutable signed entries and global reconciliation; rerun passed. Fault setup still requires a quiescent local stack. |
| Medium | Failed events need retained terminal evidence and controlled repair | Eight-attempt durable dead-letter state, precise permanent rejection reasons, explicit audited retry; attempt-limit/repair regression passed. |
| Low | Worker loop imposed its idle sleep even when useful work was available | Successful work continues immediately; idle polls and error retries retain bounded delays. |
| Low | Python cache artifacts could enter the repository/context | Removed the unpushed generated artifact from the draft milestone and excluded caches from Git and Docker context before remote publication. |

## Architecture and correctness

Separate process roles expose actual commit/HTTP/acknowledgement boundaries while one PostgreSQL instance keeps the local setup small. The shared database is explicitly a common availability dependency. The relay is not represented as Kafka or a geographically distributed broker.

Money uses bounded int64 minor units. Customer nonnegativity, balanced journals, immutable records, and same-creating-transaction entry checks are enforced in SQL. Request acceptance/outbox and inbox/posting/outcome are distinct atomic transactions. Repeatable-read reconciliation avoids comparing different committed snapshots and now verifies agreement with registered request amounts and direction.

## Reliability and security

Leases are owner-qualified and reclaimable. Outbox work is retained for indefinite capped retries; delivery dead letters remain inspectable and retryable. A process can exit after relay acknowledgement without losing the committed request; repeated transport delivery has one accounting effect. Database-owner tampering, queue-storage exhaustion, host crash durability, and cross-region availability are outside the demonstrated guarantee.

Ports bind to loopback, the app runs unprivileged, SQL uses parameters, HTTP inputs/timeouts are bounded, application JSON logs rotate, and CI token permissions are read-only. The demo role owns its schema and the API/relay trust local clients. Authentication, separated migration credentials, TLS, tenant isolation, and production financial controls remain unimplemented.

## Tests and documentation

Real PostgreSQL tests isolate schemas, reject missing dependencies rather than skipping, inject transaction failures, verify immutable journal constraints, exercise leases/owner fencing, and run repeated randomized concurrent accounting. A finite workload is not a proof over every interleaving. Runtime fault injection requires an explicit local opt-in; unsafe legacy policies exist only in test code.

The initial crash-harness overlap failure was a harness assumption, not evidence of ledger corruption; its cause and correction are recorded above. The reconciliation finding was an actual verification gap, fixed after a demonstrated failing regression. Benchmark output is scoped to local acceptance plus posting and excludes relay transport/worker scheduling. Coverage reports honestly show command/loop gaps despite live-stack exercise.

## Remaining limits

Single database; trusted local clients; USD only; no real settlement, production deployment, users, HA, automated retention, compliance approval, or independent review. Container CVE scanning and hostile traffic stress tests are additional work beyond the recorded Go dependency and credential scans. No numerical hiring score or employment guarantee is assigned.
