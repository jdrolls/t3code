# Dora autonomous-runtime adapter: intended-file manifest

## Status

Completed in this worktree. The implementation is deliberately self-contained:
Dora v1 is a credential-free JSONL child process with a documented, fail-closed
contract. No external Dora SDK, network service, account secret, push, or deploy
is required.

## Intended and changed files

- `packages/contracts/src/settings.ts` — Dora settings schema, server defaults,
  and patch schema.
- `packages/contracts/src/providerRuntime.ts` — permits the validated
  `dora.jsonl` raw source.
- `apps/server/src/provider/Services/DoraAdapter.ts` — provider adapter shape.
- `apps/server/src/provider/Layers/DoraAdapter.ts` — JSONL transport,
  work/session binding, environment isolation, event validation, lifecycle,
  receipt handling, and an injected receipt-timer seam for deterministic tests.
- `apps/server/src/provider/Drivers/DoraDriver.ts` — credential-free driver and
  instance construction.
- `apps/server/src/provider/builtInDrivers.ts` — built-in driver registration.
- `apps/server/src/provider/Layers/DoraAdapter.test.ts` — deterministic fake
  JSONL process tests, including Effect-native promise failure assertions,
  event-stream settlement, and receipt expiry without wall-clock sleeps.
- `docs/internals/dora-provider.md` — v1 wire/security contract and minimum
  receipt-timeout configuration.
- `.plans/dora-autonomous-runtime-intended-files.md` — this manifest.

## Verification and git note

Observed verification:
- Focused Dora adapter suite: 9/9 passing.
- Server typecheck: exit 0; only pre-existing Effect suggestions.
- Contracts settings suite: 31/31 passing.

No full repository build is claimed. Workspace policy prohibits staging and
committing; no Git write was attempted.
