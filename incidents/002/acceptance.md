# Executable acceptance

The unsafe completion policy fails its pending-work assertion. The corrected lost-response fixture creates two relay deliveries and one journal. The real dedicated publisher exits 77 after receipt; pending outbox work recovers with one inbox, one journal, exact signed entries, and balanced reconciliation.

Commands: `make incident ID=002` and `make test check build integration`. Also run `make recovery`. Review `internal/ledger/store.go`, `queue.go`, `worker.go`, and `schema.sql` for the actual corrective policies.

Genuine implementation milestones: `6ad16ca` (exact-money model and atomic posting) and `75b5fa8` (durable relay and crash-safe consumers). Later commits add verification and review evidence. No manufactured historical incident or collaborator approval is claimed.
