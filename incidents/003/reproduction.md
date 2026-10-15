# Reproduction

Start with `make up`, then run `make incident ID=003`. The harness runs `TestIncident003ConcurrentSpend` with the isolated test-only `LF_UNSAFE_CONCURRENCY` policy, requires the intended assertion failure, removes the flag, and requires the corrected regression to pass. Results are written to `evidence-local/`; a captured run is in `evidence/003-results.txt`.

Exact fixtures and legacy reenactments: `internal/ledger/integration_test.go`. Unsafe paths are never compiled into a service executable.
