# Dora autonomous-runtime adapter: intended-file manifest

## Status and review boundary

This worktree contains the complete T3 runtime change set for the opt-in,
credential-free Dora JSONL provider and its durable provider-session projection.
It is intended to be reviewed, committed, and opened as a PR by the
orchestrator. This manifest is the only additional change requested here; it
must remain unstaged and uncommitted for the orchestrator to stage explicitly.

Dora v1 is a local JSONL child process. It requires no Dora SDK, network
service, account secret, push, or deploy.

## Contract and security boundaries

- `providerSessionId` is an optional, bounded opaque orchestration-session
  field. It is populated only from a Dora `thread.metadata.updated` event when
  the event and active session both name the same Dora provider instance.
  Missing, malformed, generic-provider, canonical T3-thread, and provisional
  `t3-<threadId>` values are rejected. Later lifecycle updates retain the
  validated value.
- Dora's create binding uses a provisional id only on the outbound create
  request. The bridge must return a correlated ACP-minted id before its receipt;
  that id alone becomes the Dora resume cursor and projected `providerSessionId`.
- The adapter allows only a sanitized child environment, canonical absolute
  worktrees, versioned/bound JSONL records, and validated receipt/lifecycle
  ordering. It rejects secret-bearing output, malformed/oversized JSONL,
  foreign or late bindings/receipts, terminal-before-turn-receipt, unsupported
  interactions, and unexpected EOF fail-closed.
- Dora has no direct T3 database access or mutation path. Runtime events flow
  through provider-runtime ingestion and orchestration commands/event storage;
  the projection pipeline alone writes `projection_thread_sessions`.

## Intended and changed files

### Dora provider registration, contract, and hardening

- `packages/contracts/src/settings.ts` — Dora settings schema, defaults, and
  patch schema.
- `packages/contracts/src/providerRuntime.ts` — permits the validated
  `dora.jsonl` raw runtime-event source.
- `apps/server/src/provider/Services/DoraAdapter.ts` — Dora adapter service
  boundary.
- `apps/server/src/provider/Layers/DoraAdapter.ts` — JSONL transport,
  canonical work/session binding, environment isolation, secret rejection,
  handshake/receipt ordering, lifecycle cleanup, and deterministic
  receipt-timer seam.
- `apps/server/src/provider/Drivers/DoraDriver.ts` — credential-free driver
  and per-instance construction.
- `apps/server/src/provider/builtInDrivers.ts` — built-in Dora registration.
- `apps/server/src/provider/Layers/DoraAdapter.test.ts` — fake-process tests
  for protocol validation, ordering, session identity, fail-closed paths,
  receipt expiry, cleanup, and registration.
- `apps/server/src/provider/Layers/ProviderRegistry.test.ts` — registry
  expectations updated for the built-in Dora provider.
- `docs/internals/dora-provider.md` — v1 wire, security, session-identity,
  and receipt-timeout contract.

### `providerSessionId` contract, persistence, projection, and ingestion

- `packages/contracts/src/orchestration.ts` — validated public
  `ProviderSessionId` and optional `OrchestrationSession.providerSessionId`
  contract.
- `apps/server/src/persistence/Services/ProjectionThreadSessions.ts` —
  projected session row includes nullable `providerSessionId`.
- `apps/server/src/persistence/Layers/ProjectionThreadSessions.ts` — reads and
  upserts `provider_session_id` through the projection repository.
- `apps/server/src/orchestration/Layers/ProjectionPipeline.ts` — projects the
  session field from `thread.session-set` events.
- `apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts` — maps the
  projected field into full, command, shell, archived-shell, and thread-detail
  session snapshots.
- `apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.test.ts` —
  verifies projected-session snapshot hydration.
- `apps/server/src/orchestration/projector.test.ts` — exercises session
  projection behavior.
- `apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.ts` — accepts
  only validated Dora metadata bound to the active instance, dispatches the
  durable session update, and preserves it on later lifecycle updates.
- `apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.test.ts` —
  covers valid persistence plus missing-instance, malformed, provisional, and
  non-Dora rejection paths.

- `.plans/dora-autonomous-runtime-intended-files.md` — this reviewed manifest.

## Verification evidence

Observed before this manifest-only update:

- Focused Dora adapter, provider-session ingestion/projection, and provider
  registry test suites passed.
- Server typecheck completed with exit 0 (only pre-existing Effect suggestions).
- Server build completed with exit 0.

No production code, secrets, direct T3 database mutation, staging, commit, or
push was performed for this manifest refresh.
