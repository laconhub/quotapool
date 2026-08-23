# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-23

Initial release.

### Added

- `Limiter` with atomic batch admission: `Acquire` reserves all requested permits or none.
- Two quota dimensions per resource — concurrency (leased distributed semaphore) and rate (sliding window) — evaluated together in one atomic step.
- `RedisStore` for quota shared across processes, using a single Lua script that checks every constrained dimension before writing to any, and hash-tagged keys for Redis Cluster compatibility.
- `MemoryStore` for single-process deployments and tests.
- `Store` interface for custom backends.
- Fail-open by default, with `WithFailClosed()` to invert.
- `TryAcquire` for non-blocking admission.
- Exponential backoff with jitter while waiting for capacity.
- Runtime quota updates via `SetQuota`.
- `Stats` counters for admitted, rejected, fail-open, fail-closed, released and release-failed outcomes.
- Structured diagnostics through `log/slog`.

[Unreleased]: https://github.com/laconhub/quotapool/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/laconhub/quotapool/releases/tag/v0.1.0
