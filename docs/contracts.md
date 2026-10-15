# Contracts

## API

`POST /transfers`: `Idempotency-Key` is 1–128 ASCII letters/digits or `_ : . -` without spaces. All request fields are required/non-null: `from`, `to`, `amount_minor`, `currency`. Account IDs match a lowercase initial letter followed by up to 31 lowercase letters, digits, underscores or hyphens. Source and destination differ; both must be registered non-system accounts. Currency is USD. Amount is an integer in [1,1000000000000]. Request body limit: 4096 bytes; unknown fields, extra JSON values, fractional money, and malformed bodies are rejected.

HTTP 202 means durable acceptance, never synchronous financial completion. An identical retry returns the original ID/current state. Conflicting key reuse is 409. Invalid input is 400. Database uncertainty is 503 with instructions to retry the same key. Requests progress `accepted → posted` or `accepted → rejected`. Rejected outcomes are terminal until a separately designed new request is made; there is no in-place mutation of posted journals.

Read endpoints: `/accounts`, `/transfers/{uuid}`, `/journals` (latest 100), `/audit` (latest 100), `/queue` (latest 100 deliveries), `/reconcile`, `/metrics`, `/health/live`, `/health/ready`. No pagination contract beyond the stated inspection limits. Reconciliation returns 409 with a report on drift; 200 means its tested accounting comparisons agree.

## Events and queue leases

Event: `{version:1,event_id:<transfer UUID>,request:<canonical request>}`. Relay `POST /events` returns 201 only after a durable delivery append. The relay permits duplicate event IDs because transport duplication is expected. Consumers require payload identity to match the registered request before processing. Unknown or conflicting requests are permanently rejected to a retained dead-letter row.

Outbox claims and delivery claims use `FOR UPDATE SKIP LOCKED`, a random ownership token, and expiry. The default lease is 10 seconds. Posting is limited to a four-second attempt; HTTP publication to three seconds. Lease acknowledgement uses the ownership token, so an earlier owner cannot mark another worker's claim done.

Retry delay doubles from 200 milliseconds to a maximum 30 seconds. Outbox retries have no terminal attempt count, preserving committed pending requests. Delivery retries reach durable `dead` after eight attempts or immediately on a permanent event rejection. Run `docker compose run --rm --no-deps consumer retry DELIVERY_ID` only after diagnosing the recorded cause; it resets attempts and records `manual_retry`. Permanent invalid data will fail again rather than silently become valid.

## Runtime configuration

`DATABASE_URL` is required. `RELAY_URL` defaults to `http://relay:8091`. API listens on `:8090`, relay on `:8091`; `LISTEN_ADDR` overrides the relevant process. `OTEL_EXPORTER_OTLP_ENDPOINT` enables optional HTTP trace export. Compose publishes only on loopback.

`LOCAL_CRASH_AFTER_PUBLISH=1` is rejected unless `ENABLE_LOCAL_FAULTS=1`. It exits the dedicated publisher after a successful relay response and before durable outbox acknowledgement. Defaults never enable faults. Test-only `LF_UNSAFE_*` environment flags are read only by integration tests.

## Monetary invariants

Customer balances are nonnegative and account absolute balances are bounded to 1000000000000000 minor units. The system equity account may be negative to represent the exact offsetting opening funds. Journal entries are nonzero int64 deltas; PostgreSQL sums them using numeric arithmetic for balance verification. Account cached balances and ledger entries update in one transaction. Reconciliation uses a repeatable-read snapshot to avoid false drift across simultaneous commits.
