## Summary

What does this change and why?

## Type

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation
- [ ] Refactor / performance

## Checklist

- [ ] `make check` passes
- [ ] Tests cover the new behaviour (bug fixes include a test that fails without the fix)
- [ ] Exported identifiers have doc comments
- [ ] README updated if the public API changed
- [ ] CHANGELOG entry added under `[Unreleased]`

## Design constraints

Confirm none of these are weakened, or explain why the change is worth it:

- [ ] `Acquire` still admits all requested permits or none
- [ ] All quota dimensions are still checked before any is committed
- [ ] Both failure policies remain first-class
