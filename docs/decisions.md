# Decisions

## Asynchronous acceptance

The API persists transfer intent and outbox work atomically before returning 202. Posting occurs in a consumer transaction. This makes committed pending work observable through worker outages and prevents an acceptance response from falsely implying money moved. There is no reservation step; a request may later reject for insufficient funds.

## Durable HTTP relay, not Kafka

The important exercise is the boundary between source commit, external receipt, acknowledgement uncertainty, and idempotent consumption. Separate processes and independent database transactions expose that boundary clearly. One PostgreSQL instance reduces local resource use and deployment complexity. It also remains a common availability dependency; this is not a claim of database-independent distributed durability or Kafka equivalence.

## Defense in depth for replay

API key uniqueness handles repeated client intent, inbox uniqueness handles repeated events, and journal transfer uniqueness protects accounting postings independently. Fingerprints reject conflicting payloads rather than treating them as identical replays. Transport duplicates are visible as distinct relay rows.

## Stable account locking

Transfers lock their request and then both account rows sorted by ID. Stable account order avoids cross-transfer lock inversion. The second concurrent spender observes the committed balance before deciding whether to post. Database constraints and reconciliation backstop the application logic.

## Immutable journals

The initial schema uses deferred journal-balance checks, immutable posted rows, and same-creating-transaction entry checks. Corrections would require a new compensating journal; the first release exposes no correction API. Direct database-owner intervention remains outside the application trust boundary.

## Honest diagnostics

Metric labels use fixed categories, while event IDs belong in structured logs, audit records, and spans. Worker backlog is durable business state. API readiness indicates database access rather than promising that every worker is making progress. No fabricated dashboard values, external users, or production performance claims.
