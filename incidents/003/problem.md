# INCIDENT-003: Concurrent stale snapshots corrupt accounting state

Synthetic engineering reenactment, not a historical customer or production incident.

Two transfers each spend 75 from a source with 100, both observing an earlier balance.

Constraints: integer money, balanced immutable journals, retained committed requests, observable duplicate transport, and deterministic correctness criteria.
