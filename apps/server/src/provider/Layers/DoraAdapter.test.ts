import { assert, describe, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  DoraSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { DoraDriver } from "../Drivers/DoraDriver.ts";
import { ProviderAdapterValidationError } from "../Errors.ts";
import { BUILT_IN_DRIVERS } from "../builtInDrivers.ts";
import type { DoraAdapterShape } from "../Services/DoraAdapter.ts";
import {
  canonicalizeDoraWorktree,
  containsDoraSecret,
  decodeDoraEvent,
  makeDoraAdapter,
  makeNodeDoraJsonlProcess,
  sanitizeDoraEnvironment,
  type DoraJsonlProcess,
  type DoraRequest,
} from "./DoraAdapter.ts";

const decodeDoraSettings = Schema.decodeSync(DoraSettings);
const THREAD_ID = ThreadId.make("dora-test-thread");
const INSTANCE_ID = ProviderInstanceId.make("dora_test");

class FakeReceiptTimeouts {
  private readonly callbacks = new Set<() => void>();
  private readonly scheduledWaiters: Array<() => void> = [];

  readonly schedule = (_delayMs: number, callback: () => void): (() => void) => {
    this.callbacks.add(callback);
    this.scheduledWaiters.shift()?.();
    return () => this.callbacks.delete(callback);
  };

  async waitForSchedule(): Promise<void> {
    if (this.callbacks.size > 0) return;
    await new Promise<void>((resolve) => this.scheduledWaiters.push(resolve));
  }

  expireAll(): void {
    for (const callback of [...this.callbacks]) callback();
  }
}

class FakeDoraJsonlProcess implements DoraJsonlProcess {
  private readonly queued: unknown[] = [];
  private readonly waiters: Array<(result: IteratorResult<unknown>) => void> = [];
  private done = false;
  public readonly writes: DoraRequest[] = [];
  public closeCalls = 0;
  public onWrite: (request: DoraRequest) => void | Promise<void> = () => {};

  emit(event: unknown): void {
    if (this.done) return;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ done: false, value: event });
    else this.queued.push(event);
  }

  finish(): void {
    if (this.done) return;
    this.done = true;
    for (const waiter of this.waiters.splice(0)) waiter({ done: true, value: undefined });
  }

  readonly write = async (request: DoraRequest): Promise<void> => {
    this.writes.push(request);
    await this.onWrite(request);
  };

  readonly close = async (): Promise<void> => {
    this.closeCalls += 1;
    this.finish();
  };

  readonly events: AsyncIterable<unknown> = {
    [Symbol.asyncIterator]: (): AsyncIterator<unknown> => ({
      next: async (): Promise<IteratorResult<unknown>> => {
        const value = this.queued.shift();
        if (value !== undefined) return { done: false, value };
        if (this.done) return { done: true, value: undefined };
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    }),
  };
}

function makeEvent(request: DoraRequest, type: string, payload?: Record<string, unknown>, extra?: Record<string, unknown>) {
  return {
    protocolVersion: 1,
    type,
    binding: request.binding,
    ...(payload ? { payload } : {}),
    ...extra,
  };
}

function acknowledgeRequest(process: FakeDoraJsonlProcess, request: DoraRequest): void {
  if (request.op === "session.create" || request.op === "session.resume") {
    const binding =
      request.op === "session.create"
        ? { ...request.binding, sessionId: `dora-session-${request.binding.threadId}` }
        : request.binding;
    process.emit({ ...makeEvent(request, request.op === "session.create" ? "session.started" : "session.resumed"), binding });
    process.emit({ ...makeEvent(request, "receipt", undefined, { requestId: request.requestId }), binding });
  } else if (request.op === "turn") {
    process.emit(makeEvent(request, "receipt", undefined, { requestId: request.requestId }));
  } else if (request.op === "interrupt" || request.op === "approval.response" || request.op === "input.response" || request.op === "stop") {
    process.emit(makeEvent(request, "receipt", undefined, { requestId: request.requestId }));
  }
}

function makeHarness(config?: { readonly timeoutMs?: number }) {
  const process = new FakeDoraJsonlProcess();
  const receiptTimeouts = new FakeReceiptTimeouts();
  const settings = decodeDoraSettings({ binaryPath: "dora-test", requestTimeoutMs: config?.timeoutMs ?? 1_000 });
  process.onWrite = (request) => acknowledgeRequest(process, request);
  return {
    process,
    adapter: makeDoraAdapter(settings, {
      instanceId: INSTANCE_ID,
      environment: { PATH: "/safe/bin", T3_TOKEN: "must-not-pass", HOME: "/secret-home" },
      createProcess: async (input) => {
        assert.deepEqual(input.environment, { PATH: "/safe/bin" });
        return process;
      },
      scheduleReceiptTimeout: receiptTimeouts.schedule,
    }),
    receiptTimeouts,
  };
}

function start(adapter: DoraAdapterShape) {
  return adapter.startSession({
    threadId: THREAD_ID,
    provider: ProviderDriverKind.make("dora"),
    providerInstanceId: INSTANCE_ID,
    cwd: process.cwd(),
    runtimeMode: "full-access",
  });
}

describe("DoraAdapter", () => {
  it("registers Dora and decodes its built-in settings", () => {
    const settings = decodeDoraSettings({});
    assert.equal(settings.binaryPath, "dora");
    assert.equal(settings.requestTimeoutMs, 30_000);
    assert.throws(() => decodeDoraSettings({ requestTimeoutMs: 999 }));
    assert.include(BUILT_IN_DRIVERS, DoraDriver);
    assert.equal(DoraDriver.configSchema, DoraSettings);
  });

  it.effect("sanitizes child environment and validates protocol records", () =>
    Effect.gen(function* () {
      assert.deepEqual(
        sanitizeDoraEnvironment({ PATH: "/bin", LANG: "C", HOME: "/home/user", T3_TOKEN: "secret" }),
        { PATH: "/bin", LANG: "C" },
      );
      assert.isTrue(containsDoraSecret({ Authorization: "anything" }));
      assert.throws(() => decodeDoraEvent({ protocolVersion: 1, type: "receipt" }));
      assert.throws(() =>
        decodeDoraEvent({
          protocolVersion: 1,
          type: "assistant.delta",
          binding: { provider: "dora", providerInstanceId: "dora", threadId: "thread", worktree: "/tmp", sessionId: "session" },
          payload: { delta: "token=secret" },
        }),
      );
      assert.isTrue((yield* Effect.promise(() => canonicalizeDoraWorktree(process.cwd()))).length > 0);
      const failure = yield* Effect.flip(Effect.tryPromise(() => canonicalizeDoraWorktree("relative-worktree")));
      assert.equal(failure._tag, "UnknownError");
    }),
  );

  it.effect("preserves start-session validation failures", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      const adapter = yield* harness.adapter;
      const result = yield* adapter
        .startSession({
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: INSTANCE_ID,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.instanceOf(result.failure, ProviderAdapterValidationError);
        assert.equal(result.failure._tag, "ProviderAdapterValidationError");
        assert.equal(result.failure.operation, "startSession");
        assert.equal(result.failure.issue, "Provider does not match Dora.");
      }
      assert.equal(harness.process.writes.length, 0);
      assert.equal(harness.process.closeCalls, 0);
    }),
  );

  it.effect("drains child stderr while preserving JSONL stdout", () =>
    Effect.gen(function* () {
      const jsonlProcess = yield* Effect.promise(() =>
        makeNodeDoraJsonlProcess({
          binaryPath: process.execPath,
          launchArgs: [
            "-e",
            [
              'const chunk = "x".repeat(64 * 1024);',
              "let remaining = 32;",
              "const write = () => {",
              "  while (remaining-- > 0) {",
              "    if (!process.stderr.write(chunk)) {",
              '      process.stderr.once("drain", write);',
              "      return;",
              "    }",
              "  }",
              '  process.stdout.write(JSON.stringify({ protocol: "still-flowing" }) + "\\n");',
              "};",
              "write();",
            ].join("\n"),
          ],
          cwd: process.cwd(),
          environment: process.env,
        }),
      );
      const iterator = jsonlProcess.events[Symbol.asyncIterator]();
      const event = yield* Effect.promise(() => iterator.next());
      assert.deepEqual(event, { done: false, value: { protocol: "still-flowing" } });
      yield* Effect.promise(() => jsonlProcess.close());
    }),
  );

  it.effect("binds every operation to the canonical thread, worktree, instance, and Dora session", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      const adapter = yield* harness.adapter;
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const approvalOpened = yield* Deferred.make<void>();
      const inputOpened = yield* Deferred.make<void>();
      const approvalResolved = yield* Deferred.make<void>();
      const inputResolved = yield* Deferred.make<void>();
      const turnCompleted = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          runtimeEvents.push(event);
          if (event.type === "request.opened") yield* Deferred.succeed(approvalOpened, undefined).pipe(Effect.ignore);
          if (event.type === "user-input.requested") yield* Deferred.succeed(inputOpened, undefined).pipe(Effect.ignore);
          if (event.type === "request.resolved") yield* Deferred.succeed(approvalResolved, undefined).pipe(Effect.ignore);
          if (event.type === "user-input.resolved") yield* Deferred.succeed(inputResolved, undefined).pipe(Effect.ignore);
          if (event.type === "turn.completed") yield* Deferred.succeed(turnCompleted, undefined).pipe(Effect.ignore);
        }),
      ).pipe(Effect.forkChild);

      const session = yield* start(adapter);
      const canonicalWorktree = session.cwd;
      assert.deepEqual(session.resumeCursor, { schemaVersion: 1, sessionId: `dora-session-${String(THREAD_ID)}` });
      const turn = yield* adapter.sendTurn({ threadId: THREAD_ID, input: "inspect", attachments: [] });
      const turnRequest = harness.process.writes.at(-1);
      assert.isDefined(turnRequest);
      if (!turnRequest) return;

      harness.process.emit(makeEvent(turnRequest, "approval.requested", { id: "approval-1", detail: "run command" }, { turnId: String(turn.turnId) }));
      harness.process.emit(makeEvent(turnRequest, "input.requested", { id: "input-1", questions: [{ id: "scope", header: "Scope", question: "Which scope?", options: [{ label: "Workspace", description: "Current worktree" }] }] }, { turnId: String(turn.turnId) }));
      yield* Deferred.await(approvalOpened);
      yield* Deferred.await(inputOpened);

      yield* adapter.interruptTurn(THREAD_ID, turn.turnId);
      yield* adapter.respondToRequest(THREAD_ID, ApprovalRequestId.make("approval-1"), "accept");
      yield* adapter.respondToUserInput(THREAD_ID, ApprovalRequestId.make("input-1"), { scope: "Workspace" });
      yield* Deferred.await(approvalResolved);
      yield* Deferred.await(inputResolved);
      harness.process.emit(makeEvent(turnRequest, "assistant.delta", { delta: "done" }, { turnId: String(turn.turnId) }));
      harness.process.emit(makeEvent(turnRequest, "turn.completed", { state: "completed" }, { turnId: String(turn.turnId) }));
      yield* Deferred.await(turnCompleted);
      yield* adapter.stopSession(THREAD_ID);

      const operations = harness.process.writes.map((request) => request.op);
      assert.deepEqual(operations, ["session.create", "turn", "interrupt", "approval.response", "input.response", "stop"]);
      for (const [index, request] of harness.process.writes.entries()) {
        assert.equal(request.work.runId, String(THREAD_ID));
        assert.equal(request.work.worktree, canonicalWorktree);
        assert.equal(request.work.repository, canonicalWorktree);
        assert.equal(request.binding.provider, "dora");
        assert.equal(request.binding.providerInstanceId, String(INSTANCE_ID));
        assert.equal(request.binding.threadId, String(THREAD_ID));
        assert.equal(request.binding.worktree, canonicalWorktree);
        assert.equal(
          request.binding.sessionId,
          index === 0 ? `t3-${String(THREAD_ID)}` : `dora-session-${String(THREAD_ID)}`,
        );
      }
      assert.include(runtimeEvents.map((event) => event.type), "request.opened");
      assert.include(runtimeEvents.map((event) => event.type), "user-input.requested");
      assert.include(runtimeEvents.map((event) => event.type), "request.resolved");
      assert.include(runtimeEvents.map((event) => event.type), "user-input.resolved");
      assert.equal((yield* adapter.hasSession(THREAD_ID)), false);
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("persists a validated resume cursor and rejects foreign session events", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      const adapter = yield* harness.adapter;
      const runtimeFailure = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "runtime.error"
          ? Deferred.succeed(runtimeFailure, undefined).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);
      const session = yield* adapter.startSession({
        threadId: THREAD_ID,
        provider: ProviderDriverKind.make("dora"),
        providerInstanceId: INSTANCE_ID,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "durable-session" },
      });
      assert.deepEqual(session.resumeCursor, { schemaVersion: 1, sessionId: "durable-session" });
      assert.equal(harness.process.writes[0]?.op, "session.resume");
      assert.equal(harness.process.writes[0]?.binding.sessionId, "durable-session");

      const request = harness.process.writes[0];
      if (!request) return;
      harness.process.emit({ ...makeEvent(request, "assistant.delta", { delta: "foreign" }, { turnId: "turn" }), binding: { ...request.binding, sessionId: "foreign-session" } });
      yield* Deferred.await(runtimeFailure);
      assert.equal(yield* adapter.hasSession(THREAD_ID), false);
      assert.equal(harness.process.closeCalls, 1);
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("fails closed on malformed interactive events and cleans timed-out receipt state", () =>
    Effect.gen(function* () {
      const harness = makeHarness({ timeoutMs: 1_000 });
      const adapter = yield* harness.adapter;
      const timedOutRuntimeFailure = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "runtime.error"
          ? Deferred.succeed(timedOutRuntimeFailure, undefined).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* start(adapter);
      harness.process.onWrite = (request) => {
        if (request.op === "turn") return;
        harness.process.emit(makeEvent(request, "receipt", undefined, { requestId: request.requestId }));
      };
      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId: THREAD_ID, input: "will time out", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Effect.promise(() => harness.receiptTimeouts.waitForSchedule());
      harness.receiptTimeouts.expireAll();
      const failure = yield* Effect.flip(Fiber.join(sendTurnFiber));
      assert.equal(failure._tag, "ProviderAdapterRequestError");
      const timedOutTurn = harness.process.writes.at(-1);
      if (!timedOutTurn) return;
      // A late receipt cannot resurrect the deleted pending request.
      harness.process.emit(makeEvent(timedOutTurn, "receipt", undefined, { requestId: timedOutTurn.requestId }));
      yield* Deferred.await(timedOutRuntimeFailure);
      assert.equal(yield* adapter.hasSession(THREAD_ID), false);
      yield* Fiber.interrupt(eventsFiber);

      const malformedHarness = makeHarness();
      const malformedAdapter = yield* malformedHarness.adapter;
      const malformedRuntimeFailure = yield* Deferred.make<void>();
      const malformedEventsFiber = yield* Stream.runForEach(malformedAdapter.streamEvents, (event) =>
        event.type === "runtime.error"
          ? Deferred.succeed(malformedRuntimeFailure, undefined).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* start(malformedAdapter);
      const startRequest = malformedHarness.process.writes[0];
      if (!startRequest) return;
      malformedHarness.process.emit(makeEvent(startRequest, "input.requested", { id: "bad", questions: [{ id: "q" }] }, { turnId: "not-active" }));
      yield* Deferred.await(malformedRuntimeFailure);
      assert.equal(yield* malformedAdapter.hasSession(THREAD_ID), false);
      yield* Fiber.interrupt(malformedEventsFiber);
    }),
  );

  it.effect("reads, rolls back, lists, and stops every active Dora session", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      const adapter = yield* harness.adapter;
      const session = yield* start(adapter);
      const turn = yield* adapter.sendTurn({ threadId: THREAD_ID, input: "remember this", attachments: [] });

      const beforeRollback = yield* adapter.readThread(THREAD_ID);
      assert.equal(beforeRollback.turns.length, 1);
      assert.equal(beforeRollback.turns[0]?.id, turn.turnId);
      const sessionsBeforeRollback = yield* adapter.listSessions();
      assert.equal(sessionsBeforeRollback.length, 1);

      const invalidRollback = yield* Effect.result(Effect.suspend(() => adapter.rollbackThread(THREAD_ID, 0)));
      assert.isTrue(Result.isFailure(invalidRollback));
      if (Result.isFailure(invalidRollback)) {
        assert.equal(invalidRollback.failure._tag, "ProviderAdapterValidationError");
      }
      const rolledBack = yield* adapter.rollbackThread(THREAD_ID, 1);
      assert.equal(rolledBack.turns.length, 0);
      const threadAfterRollback = yield* adapter.readThread(session.threadId);
      assert.equal(threadAfterRollback.turns.length, 0);

      yield* adapter.stopAll();
      const hasSessionAfterStop = yield* adapter.hasSession(THREAD_ID);
      assert.equal(hasSessionAfterStop, false);
      const sessionsAfterStop = yield* adapter.listSessions();
      assert.equal(sessionsAfterStop.length, 0);
      assert.equal(harness.process.closeCalls, 1);
      assert.deepEqual(harness.process.writes.map((request) => request.op), [
        "session.create",
        "turn",
        "stop",
      ]);
    }),
  );

  it.effect("rejects nested secrets and malformed tool or terminal events", () =>
    Effect.gen(function* () {
      assert.throws(() =>
        decodeDoraEvent({
          protocolVersion: 1,
          type: "assistant.delta",
          binding: {
            provider: "dora",
            providerInstanceId: "dora",
            threadId: "thread",
            worktree: "/tmp",
            sessionId: "session",
          },
          payload: { nested: [{ authorization: "redacted" }] },
        }),
      );

      const toolHarness = makeHarness();
      const toolAdapter = yield* toolHarness.adapter;
      const toolRuntimeFailure = yield* Deferred.make<void>();
      const toolEventsFiber = yield* Stream.runForEach(toolAdapter.streamEvents, (event) =>
        event.type === "runtime.error"
          ? Deferred.succeed(toolRuntimeFailure, undefined).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* start(toolAdapter);
      const toolTurn = yield* toolAdapter.sendTurn({ threadId: THREAD_ID, input: "tool", attachments: [] });
      const toolRequest = toolHarness.process.writes.at(-1);
      if (!toolRequest) return;
      toolHarness.process.emit(
        makeEvent(toolRequest, "tool", { id: "tool-1", name: "Bash" }, { turnId: String(toolTurn.turnId) }),
      );
      yield* Deferred.await(toolRuntimeFailure);
      assert.equal(yield* toolAdapter.hasSession(THREAD_ID), false);
      assert.equal(toolHarness.process.closeCalls, 1);
      yield* Fiber.interrupt(toolEventsFiber);

      const turnHarness = makeHarness();
      const turnAdapter = yield* turnHarness.adapter;
      const turnRuntimeFailure = yield* Deferred.make<void>();
      const turnEventsFiber = yield* Stream.runForEach(turnAdapter.streamEvents, (event) =>
        event.type === "runtime.error"
          ? Deferred.succeed(turnRuntimeFailure, undefined).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* start(turnAdapter);
      const turn = yield* turnAdapter.sendTurn({ threadId: THREAD_ID, input: "complete", attachments: [] });
      const turnRequest = turnHarness.process.writes.at(-1);
      if (!turnRequest) return;
      turnHarness.process.emit(
        makeEvent(turnRequest, "turn.completed", { state: "unknown" }, { turnId: String(turn.turnId) }),
      );
      yield* Deferred.await(turnRuntimeFailure);
      assert.equal(yield* turnAdapter.hasSession(THREAD_ID), false);
      assert.equal(turnHarness.process.closeCalls, 1);
      yield* Fiber.interrupt(turnEventsFiber);
    }),
  );

  it.effect("cleans up failed managed processes and leaves failed spawns unregistered", () =>
    Effect.gen(function* () {
      const process = new FakeDoraJsonlProcess();
      process.onWrite = async () => {
        throw new Error("write failed");
      };
      const settings = decodeDoraSettings({ binaryPath: "dora-test", requestTimeoutMs: 1_000 });
      const adapter = yield* makeDoraAdapter(settings, {
        instanceId: INSTANCE_ID,
        createProcess: async () => process,
        scheduleReceiptTimeout: new FakeReceiptTimeouts().schedule,
      });
      const processFailure = yield* start(adapter).pipe(Effect.result);
      assert.equal(processFailure._tag, "Failure");
      if (processFailure._tag === "Failure") {
        assert.equal(processFailure.failure._tag, "ProviderAdapterProcessError");
      }
      assert.equal(process.closeCalls, 1);
      assert.equal(yield* adapter.hasSession(THREAD_ID), false);

      const spawnFailureAdapter = yield* makeDoraAdapter(settings, {
        instanceId: INSTANCE_ID,
        createProcess: async () => {
          throw new Error("spawn failed");
        },
        scheduleReceiptTimeout: new FakeReceiptTimeouts().schedule,
      });
      const spawnFailure = yield* start(spawnFailureAdapter).pipe(Effect.result);
      assert.equal(spawnFailure._tag, "Failure");
      if (spawnFailure._tag === "Failure") {
        assert.equal(spawnFailure.failure._tag, "ProviderAdapterProcessError");
      }
      assert.equal(yield* spawnFailureAdapter.hasSession(THREAD_ID), false);
      assert.equal((yield* spawnFailureAdapter.listSessions()).length, 0);
    }),
  );

  it.effect("closes a replaced session before registering its replacement", () =>
    Effect.gen(function* () {
      const processes: FakeDoraJsonlProcess[] = [];
      const settings = decodeDoraSettings({ binaryPath: "dora-test", requestTimeoutMs: 1_000 });
      const adapter = yield* makeDoraAdapter(settings, {
        instanceId: INSTANCE_ID,
        createProcess: async () => {
          const process = new FakeDoraJsonlProcess();
          process.onWrite = (request) => acknowledgeRequest(process, request);
          processes.push(process);
          return process;
        },
        scheduleReceiptTimeout: new FakeReceiptTimeouts().schedule,
      });

      const first = yield* start(adapter);
      const second = yield* adapter.startSession({
        threadId: THREAD_ID,
        provider: ProviderDriverKind.make("dora"),
        providerInstanceId: INSTANCE_ID,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: first.resumeCursor,
      });

      assert.equal(processes.length, 2);
      assert.equal(processes[0]?.closeCalls, 1);
      assert.equal(processes[1]?.closeCalls, 0);
      assert.equal(yield* adapter.hasSession(THREAD_ID), true);
      assert.equal((yield* adapter.listSessions()).length, 1);
      assert.deepEqual((yield* adapter.listSessions())[0]?.resumeCursor, second.resumeCursor);
    }),
  );
});
