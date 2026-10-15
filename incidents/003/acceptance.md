# Executable acceptance

Unsafe processing yields two postings and cache-to-journal drift. Corrected processing yields one posted request, one rejected request, source balance 25, no partial rejected journal, and balanced reconciliation.

Commands: `make incident ID=003` and `make test check build integration`. Review `internal/ledger/store.go`, `queue.go`, `worker.go`, and `schema.sql` for the actual corrective policies.

Genuine implementation milestones: `6ad16ca` (exact-money model and atomic posting) and `75b5fa8` (durable relay and crash-safe consumers). Later commits add verification and review evidence. No manufactured historical incident or collaborator approval is claimed.
