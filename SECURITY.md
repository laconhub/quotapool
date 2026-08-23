# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |

## Reporting a vulnerability

Please do not open a public issue for security reports.

Use [GitHub's private vulnerability reporting](https://github.com/laconhub/quotapool/security/advisories/new) instead. Include the affected version, a description of the issue, reproduction steps, and the impact you expect.

You can expect an initial response within 7 days and a status update within 30 days.

## Scope

This library coordinates quota state in a shared store. Two areas deserve attention in a report:

- **Key namespace collisions** — a `WithKeyPrefix` value derived from untrusted input could let one tenant read or exhaust another tenant's quota.
- **Fail-open behaviour** — the default policy admits callers when the store is unreachable. Deployments where over-admission is a security boundary rather than a capacity concern should use `WithFailClosed()`.
