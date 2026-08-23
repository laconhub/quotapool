# Contributing to quotapool

Thanks for taking the time to contribute.

## Getting started

```bash
git clone https://github.com/laconhub/quotapool.git
cd quotapool
make install
make test
```

Go 1.25 or later is required.

## Development workflow

```bash
make test              # unit tests with race detection
make test-integration  # integration tests (needs Redis)
make lint              # golangci-lint
make fmt               # gofmt + go vet
make benchmark         # benchmarks
make check             # everything CI runs
```

Run a local Redis for integration tests:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
make test-integration
```

Point them elsewhere with `QUOTAPOOL_REDIS_ADDR=host:port`.

## Before opening a pull request

- `make check` passes.
- New behaviour is covered by tests. Bug fixes include a test that fails without the fix.
- Exported identifiers have doc comments explaining *why* and *when*, not restating the signature.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `chore:`.

## Design constraints

These are load-bearing. A change that weakens one needs a strong argument in the pull request description.

**Batch admission is atomic.** `Acquire(ctx, res, n)` reserves all `n` permits or none. Any `Store` implementation must preserve this; partial admission defeats the purpose of the library.

**All dimensions are checked before any is committed.** A batch that fits concurrency but not rate must consume neither. Splitting the check and the commit lets rejected batches leak permits, which silently lowers the effective limit over time.

**Failure policy is the caller's choice.** The library must not decide unilaterally that being throttled is better than being wrong, or vice versa. Both `WithFailClosed()` and the fail-open default must stay first-class.

**The core stays dependency-light.** One direct dependency (`go-redis`) is the budget. New dependencies need justification.

## Adding a Store

Implement the two-method `Store` interface and make `Acquire` atomic across every dimension the quota constrains. Add tests mirroring `memory_test.go`, especially:

- a rejected batch leaves no permits behind;
- a batch that fits one dimension but not another is rejected without consuming either;
- expired leases free capacity;
- concurrent callers never exceed the limit.

## Reporting bugs

Open an issue with the quota configuration, the store in use, what you expected, what happened, and a reproduction if you have one. For anything security-related see [SECURITY.md](SECURITY.md).

## License

Contributions are licensed under the [Apache License 2.0](LICENSE).
