# Dora provider protocol

Dora is an opt-in, local JSONL provider. T3 starts the configured executable with
`stdin`, `stdout`, and `stderr` pipes; it never invokes a shell. This document is
the version 1 compatibility contract for the adapter and a Dora runtime.

## Configuration and isolation

`providers.dora` has `binaryPath`, `launchArgs`, and `requestTimeoutMs` settings.
`requestTimeoutMs` is validated at configuration time and must be at least 1,000
milliseconds. `launchArgs` is either empty or a JSON array of strings;
shell-style argument splitting is intentionally unsupported. Dora does not use
T3 credentials.

The child is launched with only an allowlist of execution, locale, and temporary
filesystem variables (`PATH`, Windows executable variables, `LANG`/`LC_*`,
`TERM`, and `TMP*`). In particular, it does not inherit `HOME`, T3 variables, or
any credential/provider environment variables. A Dora runtime that needs
credentials is not compatible with this adapter.

The requested `cwd` must be an absolute, existing directory. Before spawn T3
resolves it through `realpath`; that canonical path is both the worktree and
repository value. Symlink aliases and relative paths are never sent to Dora.

## Record envelope

Every stdin request and stdout event is one JSON object per line and includes:

```ts
{
  protocolVersion: 1,
  // request only
  requestId: string,
  op: "session.create" | "session.resume" | "turn" | "interrupt" |
      "approval.response" | "input.response" | "stop",
  work: { repository, issue: null, branch: null, worktree, runId },
  binding: {
    provider: "dora",
    providerInstanceId: string,
    threadId: string,
    worktree: string,
    sessionId: string
  },
  payload: Record<string, unknown>
}
```

`runId` and `binding.threadId` are the canonical T3 thread id. `work.worktree`
and `binding.worktree` are the canonical path. A new session begins with a
deterministic T3-issued provisional `sessionId`; Dora must echo it in
`session.started` before acknowledging the create request. A resumed session
uses the cursor's stored session id. Thereafter every operation, including
interrupt, interactive responses, and stop, must repeat the same binding.

## Events and ordering

Events have `protocolVersion: 1`, `type`, mandatory `binding`, and an optional
`requestId`, `turnId`, and object `payload`. Supported types are:

- `session.started` and `session.resumed`; emitted before their corresponding
  receipt and bound to the requested provider session.
- `receipt` or `failure`; both carry the original `requestId`.
- `assistant.delta`, `tool`, `approval.requested`, `input.requested`, and
  `turn.completed`; all carry the active T3 turn id exactly.

A `turn` receipt must precede its terminal event. `turn.completed.payload.state`
is exactly `completed`, `failed`, `interrupted`, or `cancelled`. Tool events need
`payload.id`, `payload.name`, and a `started`, `updated`, `completed`, or
`failed` status. Input requests need an id and non-empty question id, header,
question, and option label/description fields.

The adapter accepts one active turn per thread. It records a request only after
validating its binding and rejects responses for unknown, duplicate, wrong-kind,
or terminal requests. It emits resolved events only after Dora receipts the
response. Receipt timeouts, write failures, unknown late receipts, malformed
records, foreign bindings, unexpected stream EOF, secret-bearing output, and
unsupported events fail the session closed. Pending receipt and interaction
state is cleared on every terminal or failed lifecycle path.

## Security and observability boundary

Dora output is untrusted. The adapter rejects malformed JSONL, lines over one
million characters, unknown event types, and records containing credential-like
keys or values. It publishes only canonical runtime events after validation;
Dora payloads are never trusted as thread, worktree, instance, session, or turn
identity.

Dora v1 supports text turns, tool lifecycle events, approval responses, and
structured user input. Attachments, shell arguments, unknown interactive event
kinds, and any protocol version other than 1 are unsupported and fail closed.
The adapter stores a resume cursor only as `{ schemaVersion: 1, sessionId }` and
updates it only from a binding-validated startup event.
