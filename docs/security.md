# Security scope

Trusted local synthetic-data demonstration only. Host ports bind to loopback. The Go container runs as an unprivileged user. SQL parameters are bound, payloads and request timeouts are bounded, and CI permissions are read-only for repository contents. Account IDs are synthetic, not customer identifiers. No real financial credentials or money are used.

The tracked database password is explicitly `local_demo_only`; it is not a production secret. Store real deployment credentials only through secure runtime configuration. The demo database role owns its schema for initialization; production separation of migration and runtime privileges is not implemented.

The API and relay have no authentication, TLS termination, tenant isolation, rate limits, real settlement, or compliance certification. These missing controls prohibit treating this stack as a ready-to-use payment platform. A database owner can alter schema protections; immutable journals describe the application's enforced SQL behavior, not protection from a hostile administrator.

Go dependencies are checked with govulncheck and source/history with Gitleaks. Passing scanners do not prove absence of all vulnerabilities or sensitive strings. Container image CVE scanning and production threat modeling are additional work. Fault injection is explicitly opt-in for a dedicated local demonstration process.
