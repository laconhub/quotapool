---
name: Bug report
about: Report incorrect behaviour
title: ""
labels: bug
assignees: ""
---

**What happened**

A clear description of the incorrect behaviour.

**What you expected**

**Reproduction**

```go
// Minimal code that reproduces the issue.
```

**Configuration**

- Quota: `quotapool.Quota{...}`
- Store: `RedisStore` / `MemoryStore` / custom
- Failure policy: fail-open (default) / `WithFailClosed()`
- Batch size passed to `Acquire`:

**Environment**

- quotapool version:
- Go version:
- Redis version (if applicable):
- OS:

**Logs**

Attach output from a limiter configured with `WithLogger`, if available.
