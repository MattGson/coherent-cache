# Changelog

## 0.1.0 — Unreleased

- Initial standalone release of the typed SeaORM/PostgreSQL cache.
- Bounded process-local storage, absolute TTL, and coalesced asynchronous loads.
- Typed row-column keys with transactional invalidation for aliases, collections,
  absent rows, and `TRUNCATE`.
- Dedicated listener with heartbeat health checks and reconnect recovery.
- Commit wrapper for immediate local invalidation, including cancellation handling.
- Migration helpers and passive diagnostics.
- JSON-safe no-op row comparison in generated PostgreSQL triggers.
