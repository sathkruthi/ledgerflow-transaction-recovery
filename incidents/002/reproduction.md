# Reproduction

Start with `make up`, then run `make incident ID=002`. The harness runs `TestIncident002UncertainRelayAcknowledgement` with the isolated test-only `LF_UNSAFE_PUBLISH` policy, requires the intended assertion failure, removes the flag, and requires the corrected regression to pass. Results are written to `evidence-local/`; a captured run is in `evidence/002-results.txt`.

Exact fixtures and legacy reenactments: `internal/ledger/integration_test.go`. Unsafe paths are never compiled into a service executable.

Run `make recovery` for the real publisher process crash and automatic lease recovery. Use a disposable local stack.
