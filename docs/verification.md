# Verification

Local date: October 15, 2026, Asia/Kolkata. Linux amd64, Go 1.27.2, AMD Ryzen 7 7840HS with 16 logical CPUs and approximately 14 GiB usable RAM. GPU not used. Evidence retains actual tool timestamps.

| Check | Recorded result | Evidence |
| --- | --- | --- |
| Unit/race, formatting, static analysis, build | Passed | `evidence/final-checks.txt` |
| Real PostgreSQL integration and concurrency suite | Passed | `evidence/integration-tests.txt` |
| Three incident unsafe/fixed pairs | Intended failure followed by corrected pass | `evidence/001-results.txt` through `003-results.txt` |
| Actual publisher process crash | Exit 77; pending outbox retained; two deliveries, one journal/inbox; balanced reconciliation | `evidence/process-crash-recovery.txt` |
| Fresh isolated Compose setup/demo/recovery | Passed on separate review volumes | `evidence/clean-start-demo.json`, `clean-start-recovery.txt`, `clean-start.txt` |
| Independent request-to-journal reconciliation regression | Original implementation failed; corrected implementation passed | `evidence/reconciliation-before-fix.txt`, `reconciliation-after-fix.txt` |
| Go vulnerability scan | No vulnerabilities found | `evidence/dependency-scan.txt` |
| Credential-pattern files/history checks | No leaks reported | `evidence/secret-scan-files.txt`, `secret-scan-history.txt` |
| Fuzz request validation/replay identity | 44980 executions, no failures | `evidence/fuzz.txt` |
| Instrumented coverage | Actual output retained; entrypoint and long-running worker loop are not instrumented by unit coverage | `evidence/coverage.txt` |

## Benchmark scope

Three requested three-second samples measured approximately **4.490630**, **4.155080**, and **4.358833 milliseconds** per request acceptance plus ledger posting. Each operation commits acceptance/outbox, then commits posting/inbox/accounting. The benchmark reports approximately 12972–12982 allocated bytes and 378 Go allocations per operation. It uses its own isolated schema and cleans it up.

These are sequential local database measurements. They exclude HTTP event relay, worker scheduling, end-to-end settlement latency, distributed failover, and production capacity. Other host/container activity was not isolated. The Go harness calibrates sample lengths and warm-up; raw counts and output remain in `evidence/benchmark.txt`. No service-level objective is inferred.

## Honest boundaries

Finite randomized tests, short fuzzing, and a passing scan do not certify all runtime behavior or security. The data is synthetic; account names are fixtures. Real GitHub CI and repository visibility must be verified separately rather than inferred from the workflow file. Publication requires separate owner approval for this repository.
