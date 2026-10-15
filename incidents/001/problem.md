# INCIDENT-001: Duplicate event creates duplicate accounting posting

Synthetic engineering reenactment, not a historical customer or production incident.

A transfer event is delivered twice. A legacy consumer re-applies its business effect after each receipt.

Constraints: integer money, balanced immutable journals, retained committed requests, observable duplicate transport, and deterministic correctness criteria.
