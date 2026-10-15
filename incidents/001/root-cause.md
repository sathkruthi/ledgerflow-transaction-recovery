# Root cause

The test-only legacy replay path omits the durable replay gate and runs in an isolated schema with the independent journal uniqueness guard explicitly removed. This accurately exposes the missing protections without weakening the executable service. Correct processing serializes the request, checks the inbox fingerprint, and enforces one journal per transfer.
