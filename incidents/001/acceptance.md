# Executable acceptance

Unsafe replay yields two journals and a double debit. Corrected processing produces one journal, source balance 75 from an opening 100, and balanced reconciliation.

Commands: `make incident ID=001` and `make test check build integration`. Review `internal/ledger/store.go`, `queue.go`, `worker.go`, and `schema.sql` for the actual corrective policies.

Genuine implementation milestones: `6ad16ca` (exact-money model and atomic posting) and `75b5fa8` (durable relay and crash-safe consumers). Later commits add verification and review evidence. No manufactured historical incident or collaborator approval is claimed.
