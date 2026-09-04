// @effect-diagnostics nodeBuiltinImport:off
// The JSONL child boundary deliberately owns its receipt clock and emits
// host timestamps from synchronous transport callbacks.
// @effect-diagnostics globalTimers:off
// @effect-diagnostics globalDate:off
// @effect-diagnostics globalDateInEffect:off
// @effect-diagnostics runEffectInsideEffect:off
/**
 * DoraAdapter — versioned, credential-free JSONL child-process adapter.
 *
 * The child receives a sanitized environment and every JSONL record is bound
 * to one canonical T3 thread, worktree, provider instance, and Dora session.
 * See docs/internals/dora-provider.md for the wire contract.
 */
import { realpath, stat } from "node:fs/promises";
import * as NodePath from "node:path";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import * as Readline from "node:readline";

import {
  ApprovalRequestId,
  EventId,
  type DoraSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { DoraAdapterShape } from "../Services/DoraAdapter.ts";
import type { ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";

const isProviderAdapterValidationError = Schema.is(ProviderAdapterValidationError);
const PROVIDER = ProviderDriverKind.make("dora");
const PROTOCOL_VERSION = 1 as const;
const DORA_RESUME_VERSION = 1 as const;
const MAX_JSONL_LINE_CHARS = 1_000_000;
const SAFE_CHILD_ENVIRONMENT = new Set([
  "PATH",
  "PATHEXT",
  "SYSTEMROOT",
  "COMSPEC",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "TERM",
  "TMPDIR",
  "TMP",
  "TEMP",
]);

export type DoraOperation =
  | "session.create"
  | "session.resume"
  | "turn"
  | "interrupt"
  | "approval.response"
  | "input.response"
  | "stop";

export interface DoraWorkIdentity {
  readonly repository: string;
  readonly issue: string | null;
  readonly branch: string | null;
  readonly worktree: string;
  /** Canonical T3 thread identifier, retained as Dora's run id. */
  readonly runId: string;
}

/** Every request and event binds the same T3 and provider-session identity. */
export interface DoraBinding {
  readonly provider: "dora";
  readonly providerInstanceId: string;
  readonly threadId: string;
  readonly worktree: string;
  readonly sessionId: string;
}

export interface DoraRequest {
  readonly protocolVersion: typeof PROTOCOL_VERSION;
  readonly requestId: string;
  readonly op: DoraOperation;
  readonly work: DoraWorkIdentity;
  readonly binding: DoraBinding;
  readonly payload: Record<string, unknown>;
}

/** A small transport seam makes protocol tests deterministic without a shell. */
export interface DoraJsonlProcess {
  readonly write: (request: DoraRequest) => Promise<void>;
  readonly events: AsyncIterable<unknown>;
  readonly close: () => Promise<void>;
}

export interface DoraAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  /** Input is filtered by sanitizeDoraEnvironment before child creation. */
  readonly environment?: NodeJS.ProcessEnv;
  readonly createProcess?: (input: {
    readonly binaryPath: string;
    readonly launchArgs: ReadonlyArray<string>;
    readonly cwd: string;
    readonly environment: NodeJS.ProcessEnv;
  }) => Promise<DoraJsonlProcess>;
  /** Test seam for deterministic receipt-expiry behavior. */
  readonly scheduleReceiptTimeout?: (delayMs: number, callback: () => void) => () => void;
}

interface PendingReceipt {
  readonly resolve: () => void;
  readonly reject: (cause: Error) => void;
}

interface PendingInteraction {
  readonly kind: "approval" | "input";
  readonly turnId: TurnId;
}

interface DoraContext {
  readonly threadId: ThreadId;
  readonly process: DoraJsonlProcess;
  readonly work: DoraWorkIdentity;
  readonly providerInstanceId: ProviderInstanceId;
  session: ProviderSession;
  providerSessionId: string | undefined;
  activeTurnId: TurnId | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly receipts: Map<string, PendingReceipt>;
  readonly interactions: Map<ApprovalRequestId, PendingInteraction>;
  /** Shared so concurrent failure and cleanup paths close the child once. */
  closePromise: Promise<void> | undefined;
  stopped: boolean;
}

interface DoraEvent {
  readonly protocolVersion: typeof PROTOCOL_VERSION;
  readonly type:
    | "receipt"
    | "session.started"
    | "session.resumed"
    | "assistant.delta"
    | "tool"
    | "approval.requested"
    | "input.requested"
    | "turn.completed"
    | "failure";
  readonly binding: DoraBinding;
  readonly requestId?: string;
  readonly turnId?: string;
  readonly payload?: Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

/** Dora gets only process-locale/executable-temp variables, never T3 credentials. */
export function sanitizeDoraEnvironment(input: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  for (const key of SAFE_CHILD_ENVIRONMENT) {
    const value = input[key];
    if (value !== undefined) environment[key] = value;
  }
  return environment;
}

/** Resolve symlinks and reject non-directory or relative worktree inputs. */
export async function canonicalizeDoraWorktree(cwd: string): Promise<string> {
  const trimmed = cwd.trim();
  if (!trimmed || !NodePath.isAbsolute(trimmed)) {
    throw new Error("Dora requires an absolute worktree path.");
  }
  const canonical = await realpath(trimmed);
  if (!(await stat(canonical)).isDirectory()) {
    throw new Error("Dora worktree path must be a directory.");
  }
  return canonical;
}

/** Refuse records that could leak secrets into persistent runtime events. */
export function containsDoraSecret(value: unknown, seen = new Set<unknown>()): boolean {
  if (typeof value === "string") {
    return /(?:api[_-]?key|authorization|bearer\s+[a-z0-9._~+/=-]{8,}|secret|password|token)\s*[:=]/iu.test(
      value,
    );
  }
  if (!isRecord(value) && !Array.isArray(value)) return false;
  if (seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some((entry) => containsDoraSecret(entry, seen));
  return Object.entries(value).some(
    ([key, entry]) =>
      /(?:api[_-]?key|authorization|secret|password|credential|access[_-]?token|refresh[_-]?token|token)/iu.test(key) ||
      containsDoraSecret(entry, seen),
  );
}

export interface DoraTurnCompletedPayload {
  readonly state: "completed" | "failed" | "interrupted" | "cancelled";
  readonly stopReason?: string | null;
  readonly usage?: unknown;
  readonly modelUsage?: Record<string, unknown>;
  readonly totalCostUsd?: number;
  readonly errorMessage?: string;
}

/** Validate Dora's terminal payload against the canonical provider event contract. */
export function decodeDoraTurnCompletedPayload(value: unknown): DoraTurnCompletedPayload {
  if (!isRecord(value)) throw new Error("Dora turn.completed payload must be an object.");
  const state = value.state;
  if (state !== "completed" && state !== "failed" && state !== "interrupted" && state !== "cancelled") {
    throw new Error("Dora turn.completed is malformed.");
  }
  const stopReason = value.stopReason;
  const parsedStopReason = nonEmptyString(stopReason);
  if (stopReason !== undefined && stopReason !== null && !parsedStopReason) {
    throw new Error("Dora turn.completed stopReason is malformed.");
  }
  const modelUsage = value.modelUsage;
  if (modelUsage !== undefined && !isRecord(modelUsage)) {
    throw new Error("Dora turn.completed modelUsage is malformed.");
  }
  const totalCostUsd = value.totalCostUsd;
  if (totalCostUsd !== undefined && (typeof totalCostUsd !== "number" || !Number.isFinite(totalCostUsd))) {
    throw new Error("Dora turn.completed totalCostUsd is malformed.");
  }
  const message = nonEmptyString(value.message);
  if (value.message !== undefined && !message) {
    throw new Error("Dora turn.completed message is malformed.");
  }
  return {
    state,
    ...(stopReason === null ? { stopReason: null } : parsedStopReason ? { stopReason: parsedStopReason } : {}),
    ...(Object.hasOwn(value, "usage") ? { usage: value.usage } : {}),
    ...(modelUsage ? { modelUsage } : {}),
    ...(totalCostUsd !== undefined ? { totalCostUsd } : {}),
    ...(message ? { errorMessage: message } : {}),
  };
}

function decodeBinding(value: unknown): DoraBinding {
  if (!isRecord(value)) throw new Error("Dora event is missing binding.");
  const providerInstanceId = nonEmptyString(value.providerInstanceId);
  const threadId = nonEmptyString(value.threadId);
  const worktree = nonEmptyString(value.worktree);
  const sessionId = nonEmptyString(value.sessionId);
  if (value.provider !== "dora" || !providerInstanceId || !threadId || !worktree || !sessionId) {
    throw new Error("Dora event binding is malformed.");
  }
  return { provider: "dora", providerInstanceId, threadId, worktree, sessionId };
}

export function decodeDoraEvent(value: unknown): DoraEvent {
  if (!isRecord(value) || value.protocolVersion !== PROTOCOL_VERSION || typeof value.type !== "string") {
    throw new Error("Dora emitted a malformed protocol event.");
  }
  if (containsDoraSecret(value)) throw new Error("Dora emitted secret-bearing protocol output.");
  const allowed = new Set<DoraEvent["type"]>([
    "receipt",
    "session.started",
    "session.resumed",
    "assistant.delta",
    "tool",
    "approval.requested",
    "input.requested",
    "turn.completed",
    "failure",
  ]);
  if (!allowed.has(value.type as DoraEvent["type"])) throw new Error("Dora emitted an unsupported protocol event.");
  const requestId = nonEmptyString(value.requestId);
  const turnId = nonEmptyString(value.turnId);
  const payload = value.payload === undefined ? undefined : isRecord(value.payload) ? value.payload : undefined;
  if (value.payload !== undefined && payload === undefined) throw new Error("Dora event payload must be an object.");
  if ((value.type === "receipt" || value.type === "failure") && !requestId) {
    throw new Error("Dora receipt/failure event is missing requestId.");
  }
  return {
    protocolVersion: PROTOCOL_VERSION,
    type: value.type as DoraEvent["type"],
    binding: decodeBinding(value.binding),
    ...(requestId ? { requestId } : {}),
    ...(turnId ? { turnId } : {}),
    ...(payload ? { payload } : {}),
  };
}

function parseLaunchArgs(value: string): ReadonlyArray<string> {
  if (!value.trim()) return [];
  try {
    const parsed: unknown = JSON.parse(value);
    if (Array.isArray(parsed) && parsed.every((entry) => typeof entry === "string")) return parsed;
  } catch {
    // Fail with the stable configuration diagnostic below.
  }
  throw new Error("Dora launchArgs must be a JSON array of strings.");
}

export function makeNodeDoraJsonlProcess(input: {
  readonly binaryPath: string;
  readonly launchArgs: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment: NodeJS.ProcessEnv;
}): Promise<DoraJsonlProcess> {
  return new Promise((resolve, reject) => {
    const child: ChildProcessWithoutNullStreams = spawn(input.binaryPath, [...input.launchArgs], {
      cwd: input.cwd,
      env: sanitizeDoraEnvironment(input.environment),
      shell: false,
      stdio: ["pipe", "pipe", "pipe"],
    });
    // stderr is outside the JSONL protocol. Keep it flowing so a noisy child
    // cannot fill its pipe and block stdout/protocol progress; never retain it.
    child.stderr.resume();
    const onError = () => reject(new Error("Unable to start Dora child process."));
    child.once("error", onError);
    child.once("spawn", () => {
      child.off("error", onError);
      const lines = Readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
      const events = (async function* () {
        for await (const line of lines) {
          if (line.length > MAX_JSONL_LINE_CHARS) throw new Error("Dora JSONL line exceeds limit.");
          try {
            yield JSON.parse(line) as unknown;
          } catch {
            throw new Error("Dora emitted invalid JSONL.");
          }
        }
      })();
      resolve({
        write: (request) =>
          new Promise((writeResolve, writeReject) => {
            child.stdin.write(`${JSON.stringify(request)}\n`, (error) =>
              error ? writeReject(error) : writeResolve(),
            );
          }),
        events,
        close: () =>
          new Promise((closeResolve) => {
            if (child.exitCode !== null || child.killed) return closeResolve();
            child.once("exit", () => closeResolve());
            child.kill("SIGTERM");
          }),
      });
    });
  });
}

function parseResumeCursor(value: unknown): { readonly sessionId: string } | undefined {
  if (!isRecord(value) || value.schemaVersion !== DORA_RESUME_VERSION) return undefined;
  const sessionId = nonEmptyString(value.sessionId);
  return sessionId ? { sessionId } : undefined;
}

function detail(payload: Record<string, unknown> | undefined, fallback: string): string {
  const message = nonEmptyString(payload?.message);
  return message ?? fallback;
}

function toolItemType(name: string): "command_execution" | "file_change" | "dynamic_tool_call" {
  const lower = name.toLowerCase();
  if (lower.includes("command") || lower.includes("shell") || lower.includes("bash")) return "command_execution";
  if (lower.includes("write") || lower.includes("edit") || lower.includes("patch")) return "file_change";
  return "dynamic_tool_call";
}

export function makeDoraAdapter(settings: DoraSettings, options?: DoraAdapterLiveOptions) {
  return Effect.gen(function* () {
    const instanceId = options?.instanceId ?? ProviderInstanceId.make("dora");
    const scheduleReceiptTimeout = options?.scheduleReceiptTimeout ?? ((delayMs: number, callback: () => void) => {
      const timer = setTimeout(callback, delayMs);
      return () => clearTimeout(timer);
    });
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, DoraContext>();
    let counter = 0;
    const stamp = () => ({ eventId: EventId.make(`dora-${Date.now()}-${counter++}`), createdAt: new Date().toISOString() });
    const emit = (event: ProviderRuntimeEvent) => Effect.runFork(Queue.offer(runtimeEvents, event));
    const bindingFor = (ctx: DoraContext): DoraBinding => {
      if (!ctx.providerSessionId) throw new Error("Dora session has not acknowledged a provider session id.");
      return {
        provider: "dora",
        providerInstanceId: String(ctx.providerInstanceId),
        threadId: String(ctx.threadId),
        worktree: ctx.work.worktree,
        sessionId: ctx.providerSessionId,
      };
    };
    const restoreReady = (ctx: DoraContext) => {
      const { activeTurnId: _activeTurnId, ...session } = ctx.session;
      ctx.session = { ...session, status: "ready", updatedAt: new Date().toISOString() };
      ctx.activeTurnId = undefined;
      ctx.interactions.clear();
    };
    const rejectReceipts = (ctx: DoraContext, cause: Error) => {
      for (const [requestId, pending] of ctx.receipts) {
        ctx.receipts.delete(requestId);
        pending.reject(cause);
      }
    };
    const closeContext = (ctx: DoraContext): Promise<void> => {
      ctx.closePromise ??= ctx.process.close();
      return ctx.closePromise;
    };
    const fail = (ctx: DoraContext, message: string) => {
      if (ctx.stopped) return;
      ctx.stopped = true;
      sessions.delete(ctx.threadId);
      rejectReceipts(ctx, new Error(message));
      const activeTurnId = ctx.activeTurnId;
      if (activeTurnId) {
        restoreReady(ctx);
        emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId: activeTurnId, type: "turn.completed", payload: { state: "failed", errorMessage: message } });
      }
      emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, type: "runtime.error", payload: { message, class: "transport_error" } });
      void closeContext(ctx);
    };
    const validateEventBinding = (ctx: DoraContext, event: DoraEvent) => {
      const binding = event.binding;
      if (
        binding.providerInstanceId !== String(ctx.providerInstanceId) ||
        binding.threadId !== String(ctx.threadId) ||
        binding.worktree !== ctx.work.worktree
      ) {
        throw new Error("Dora event binding does not match the active T3 session.");
      }
      const isSessionHandshake = event.type === "session.started" || event.type === "session.resumed";
      if (ctx.providerSessionId === undefined && !isSessionHandshake) {
        throw new Error("Dora emitted a non-start event before binding a provider session.");
      }
      if (ctx.providerSessionId !== undefined && binding.sessionId !== ctx.providerSessionId) {
        throw new Error("Dora event session does not match the active provider session.");
      }
      if (isSessionHandshake) {
        ctx.providerSessionId = binding.sessionId;
        ctx.session = {
          ...ctx.session,
          resumeCursor: { schemaVersion: DORA_RESUME_VERSION, sessionId: binding.sessionId },
          updatedAt: new Date().toISOString(),
        };
      }
    };
    const activeTurnFor = (ctx: DoraContext, event: DoraEvent): TurnId => {
      if (!ctx.activeTurnId || event.turnId !== String(ctx.activeTurnId)) {
        throw new Error("Dora event does not belong to the active turn.");
      }
      return ctx.activeTurnId;
    };
    const consume = async (ctx: DoraContext) => {
      try {
        for await (const raw of ctx.process.events) {
          if (ctx.stopped) return;
          const event = decodeDoraEvent(raw);
          validateEventBinding(ctx, event);
          if (event.type === "receipt" || event.type === "failure") {
            const pending = event.requestId ? ctx.receipts.get(event.requestId) : undefined;
            if (!pending) throw new Error("Dora emitted a receipt for an unknown request.");
            ctx.receipts.delete(event.requestId!);
            event.type === "receipt"
              ? pending.resolve()
              : pending.reject(new Error(detail(event.payload, "Dora rejected request.")));
          }
          switch (event.type) {
            case "session.started":
            case "session.resumed":
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, type: "thread.started", payload: { providerThreadId: ctx.providerSessionId } });
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, type: "thread.metadata.updated", payload: { metadata: { doraSessionId: ctx.providerSessionId, work: ctx.work, resumed: event.type === "session.resumed" } } });
              break;
            case "assistant.delta": {
              const turnId = activeTurnFor(ctx, event);
              const delta = nonEmptyString(event.payload?.delta);
              if (!delta) throw new Error("Dora assistant.delta is missing delta.");
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId, type: "content.delta", payload: { streamKind: "assistant_text", delta }, raw: { source: "dora.jsonl", payload: raw } });
              break;
            }
            case "tool": {
              const turnId = activeTurnFor(ctx, event);
              const id = nonEmptyString(event.payload?.id);
              const name = nonEmptyString(event.payload?.name);
              const status = event.payload?.status;
              if (!id || !name || (status !== "started" && status !== "updated" && status !== "completed" && status !== "failed")) throw new Error("Dora tool event is malformed.");
              const type = status === "started" ? "item.started" : status === "updated" ? "item.updated" : "item.completed";
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId, itemId: RuntimeItemId.make(id), type, payload: { itemType: toolItemType(name), status: status === "started" || status === "updated" ? "inProgress" : status, title: name, data: event.payload }, raw: { source: "dora.jsonl", payload: raw } });
              break;
            }
            case "approval.requested": {
              const turnId = activeTurnFor(ctx, event);
              const requestIdValue = nonEmptyString(event.payload?.id);
              if (!requestIdValue) throw new Error("Dora approval request is malformed or duplicated.");
              const requestId = ApprovalRequestId.make(requestIdValue);
              if (ctx.interactions.has(requestId)) throw new Error("Dora approval request is malformed or duplicated.");
              ctx.interactions.set(requestId, { kind: "approval", turnId });
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId, requestId: RuntimeRequestId.make(requestId), type: "request.opened", payload: { requestType: "dynamic_tool_call", ...(nonEmptyString(event.payload?.detail) ? { detail: nonEmptyString(event.payload?.detail) } : {}) }, raw: { source: "dora.jsonl", payload: raw } });
              break;
            }
            case "input.requested": {
              const turnId = activeTurnFor(ctx, event);
              const requestIdValue = nonEmptyString(event.payload?.id);
              const questions = event.payload?.questions;
              if (!requestIdValue || !Array.isArray(questions) || questions.some((q) => !isRecord(q) || !nonEmptyString(q.id) || !nonEmptyString(q.header) || !nonEmptyString(q.question) || !Array.isArray(q.options) || q.options.some((option) => !isRecord(option) || !nonEmptyString(option.label) || !nonEmptyString(option.description)))) {
                throw new Error("Dora input request is malformed or unsupported.");
              }
              const requestId = ApprovalRequestId.make(requestIdValue);
              if (ctx.interactions.has(requestId)) throw new Error("Dora input request is malformed or unsupported.");
              ctx.interactions.set(requestId, { kind: "input", turnId });
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId, requestId: RuntimeRequestId.make(requestId), type: "user-input.requested", payload: { questions: questions as never }, raw: { source: "dora.jsonl", payload: raw } });
              break;
            }
            case "turn.completed": {
              const turnId = activeTurnFor(ctx, event);
              const payload = decodeDoraTurnCompletedPayload(event.payload);
              restoreReady(ctx);
              emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: ctx.threadId, turnId, type: "turn.completed", payload, raw: { source: "dora.jsonl", payload: raw } });
              break;
            }
            case "failure":
              fail(ctx, detail(event.payload, "Dora runtime failed."));
              break;
            case "receipt":
              break;
          }
        }
        if (!ctx.stopped) fail(ctx, "Dora protocol stream ended unexpectedly.");
      } catch {
        fail(ctx, "Dora protocol stream failed validation.");
      }
    };
    const request = async (
      ctx: DoraContext,
      op: DoraOperation,
      payload: Record<string, unknown>,
      initialBinding?: DoraBinding,
    ) => {
      const requestId = `dora-${Date.now()}-${counter++}`;
      // A fresh create request is the sole request allowed to use the
      // provisional id. session.started establishes the durable id instead.
      const binding = initialBinding ?? bindingFor(ctx);
      await new Promise<void>((resolve, reject) => {
        const settle = (cause?: Error) => {
          const pending = ctx.receipts.get(requestId);
          if (!pending) return;
          ctx.receipts.delete(requestId);
          cause ? pending.reject(cause) : pending.resolve();
        };
        const cancelTimeout = scheduleReceiptTimeout(settings.requestTimeoutMs, () =>
          settle(new Error("Dora protocol receipt timed out.")),
        );
        ctx.receipts.set(requestId, {
          resolve: () => { cancelTimeout(); resolve(); },
          reject: (cause) => { cancelTimeout(); reject(cause); },
        });
        void ctx.process.write({ protocolVersion: PROTOCOL_VERSION, requestId, op, work: ctx.work, binding, payload }).catch((cause) => settle(cause instanceof Error ? cause : new Error("Dora protocol write failed.")));
      });
    };
    const requireSession = (threadId: ThreadId) => {
      const ctx = sessions.get(threadId);
      return ctx && !ctx.stopped ? ctx : undefined;
    };
    const startSession: DoraAdapterShape["startSession"] = (input) => {
      if (input.modelSelection !== undefined && input.modelSelection.instanceId !== instanceId) {
        return Effect.fail(new ProviderAdapterValidationError({ provider: PROVIDER, operation: "startSession", issue: "Model selection belongs to a different provider instance." }));
      }
      return Effect.tryPromise({
      try: async () => {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          throw new ProviderAdapterValidationError({ provider: PROVIDER, operation: "startSession", issue: "Provider does not match Dora." });
        }
        if (input.providerInstanceId !== undefined && input.providerInstanceId !== instanceId) {
          throw new ProviderAdapterValidationError({ provider: PROVIDER, operation: "startSession", issue: "Provider instance does not match Dora." });
        }
        if (!input.cwd) {
          throw new ProviderAdapterValidationError({ provider: PROVIDER, operation: "startSession", issue: "cwd is required." });
        }
        let cwd: string;
        try {
          cwd = await canonicalizeDoraWorktree(input.cwd);
        } catch (cause) {
          throw new ProviderAdapterValidationError({ provider: PROVIDER, operation: "startSession", issue: "cwd must be an absolute existing directory.", cause });
        }
        const old = requireSession(input.threadId);
        if (old) { old.stopped = true; rejectReceipts(old, new Error("Dora session was replaced.")); sessions.delete(input.threadId); await closeContext(old); }
        const resume = parseResumeCursor(input.resumeCursor);
        const work: DoraWorkIdentity = { repository: cwd, issue: null, branch: null, worktree: cwd, runId: String(input.threadId) };
        const binding: DoraBinding = { provider: "dora", providerInstanceId: String(instanceId), threadId: String(input.threadId), worktree: cwd, sessionId: resume?.sessionId ?? `t3-${String(input.threadId)}` };
        const process = await (options?.createProcess ?? makeNodeDoraJsonlProcess)({ binaryPath: settings.binaryPath, launchArgs: parseLaunchArgs(settings.launchArgs), cwd, environment: sanitizeDoraEnvironment(options?.environment) });
        const now = new Date().toISOString();
        const ctx: DoraContext = { threadId: input.threadId, process, work, providerInstanceId: instanceId, providerSessionId: resume?.sessionId, activeTurnId: undefined, turns: [], receipts: new Map(), interactions: new Map(), closePromise: undefined, stopped: false, session: { provider: PROVIDER, providerInstanceId: instanceId, status: "ready", runtimeMode: input.runtimeMode, cwd, ...(input.modelSelection ? { model: input.modelSelection.model } : {}), threadId: input.threadId, ...(resume ? { resumeCursor: { schemaVersion: DORA_RESUME_VERSION, sessionId: resume.sessionId } } : {}), createdAt: now, updatedAt: now } };
        sessions.set(input.threadId, ctx);
        void consume(ctx);
        try {
          await request(
            ctx,
            resume ? "session.resume" : "session.create",
            resume
              ? { runtimeMode: input.runtimeMode }
              : { runtimeMode: input.runtimeMode, ...(input.modelSelection ? { model: input.modelSelection.model } : {}) },
            // The create request needs a binding before Dora has supplied an
            // id. Its provisional id is not stored in the live context.
            resume ? undefined : binding,
          );
        } catch (cause) {
          ctx.stopped = true;
          sessions.delete(ctx.threadId);
          rejectReceipts(ctx, cause instanceof Error ? cause : new Error("Dora session start failed."));
          await closeContext(ctx);
          throw cause;
        }
        emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: input.threadId, type: "session.started", payload: { message: resume ? "Dora session resumed" : "Dora session started", resume: ctx.session.resumeCursor } });
        return ctx.session;
      },
      catch: (cause) =>
        isProviderAdapterValidationError(cause)
          ? cause
          : new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: "Failed to start Dora runtime session.",
              cause,
            }),
      });
    };
    const sendTurn: DoraAdapterShape["sendTurn"] = (input) => {
      if (input.modelSelection !== undefined && input.modelSelection.instanceId !== instanceId) {
        return Effect.fail(new ProviderAdapterValidationError({ provider: PROVIDER, operation: "sendTurn", issue: "Model selection belongs to a different provider instance." }));
      }
      return Effect.tryPromise({
      try: async () => {
        const ctx = requireSession(input.threadId); if (!ctx) throw new Error("session not found");
        if (ctx.activeTurnId) throw new Error("Dora does not accept a new turn while another turn is active.");
        if (input.attachments?.length) throw new Error("Dora JSONL v1 does not support attachments.");
        const text = input.input?.trim(); if (!text) throw new Error("Dora turns require non-empty text.");
        const turnId = TurnId.make(`dora-turn-${Date.now()}-${counter++}`);
        ctx.activeTurnId = turnId;
        ctx.session = { ...ctx.session, status: "running", activeTurnId: turnId, updatedAt: new Date().toISOString() };
        ctx.turns.push({ id: turnId, items: [{ input: text }] });
        emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: input.threadId, turnId, type: "turn.started", payload: {} });
        try {
          await request(ctx, "turn", { turnId: String(turnId), input: text, ...(input.modelSelection ? { model: input.modelSelection.model } : {}) });
        } catch (cause) {
          if (!ctx.stopped && ctx.activeTurnId === turnId) {
            restoreReady(ctx);
            emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId: input.threadId, turnId, type: "turn.completed", payload: { state: "failed", errorMessage: "Dora turn request failed." } });
          }
          throw cause;
        }
        return { threadId: input.threadId, turnId, ...(ctx.session.resumeCursor ? { resumeCursor: ctx.session.resumeCursor } : {}) };
      },
      catch: (cause) => new ProviderAdapterRequestError({ provider: PROVIDER, method: "turn", detail: "Dora turn request failed.", cause }),
      });
    };
    const command = (threadId: ThreadId, op: DoraOperation, payload: Record<string, unknown>) => Effect.tryPromise({ try: async () => { const ctx = requireSession(threadId); if (!ctx) throw new Error("session not found"); await request(ctx, op, payload); }, catch: (cause) => new ProviderAdapterRequestError({ provider: PROVIDER, method: op, detail: "Dora protocol request failed.", cause }) });
    const stopSession: DoraAdapterShape["stopSession"] = (threadId) => Effect.tryPromise({ try: async () => { const ctx = requireSession(threadId); if (!ctx) throw new Error("session not found"); await request(ctx, "stop", {}); ctx.stopped = true; sessions.delete(threadId); rejectReceipts(ctx, new Error("Dora session stopped.")); await closeContext(ctx); emit({ ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId, type: "session.exited", payload: { exitKind: "graceful" } }); }, catch: (cause) => new ProviderAdapterRequestError({ provider: PROVIDER, method: "stop", detail: "Dora stop request failed.", cause }) });
    const respondToInteraction = (threadId: ThreadId, requestId: ApprovalRequestId, kind: PendingInteraction["kind"], payload: Record<string, unknown>) => Effect.tryPromise({ try: async () => { const ctx = requireSession(threadId); if (!ctx) throw new Error("session not found"); const pending = ctx.interactions.get(requestId); if (!pending || pending.kind !== kind) throw new Error("Dora interactive request is unknown or has the wrong type."); await request(ctx, kind === "approval" ? "approval.response" : "input.response", { requestId, ...payload }); ctx.interactions.delete(requestId); emit(kind === "approval" ? { ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId, turnId: pending.turnId, requestId: RuntimeRequestId.make(requestId), type: "request.resolved", payload: { requestType: "dynamic_tool_call", decision: String(payload.decision) } } : { ...stamp(), provider: PROVIDER, providerInstanceId: instanceId, threadId, turnId: pending.turnId, requestId: RuntimeRequestId.make(requestId), type: "user-input.resolved", payload: { answers: payload.answers as Record<string, unknown> } }); }, catch: (cause) => new ProviderAdapterRequestError({ provider: PROVIDER, method: kind === "approval" ? "approval.response" : "input.response", detail: "Dora interactive response failed.", cause }) });
    yield* Effect.addFinalizer(() => Effect.promise(() => Promise.all([...sessions.values()].map(async (ctx) => { ctx.stopped = true; rejectReceipts(ctx, new Error("Dora adapter closed.")); await closeContext(ctx); }))).pipe(Effect.asVoid, Effect.ensuring(Queue.shutdown(runtimeEvents))));
    const rollbackThread = (
      threadId: ThreadId,
      numTurns: number,
    ): Effect.Effect<
      ProviderThreadSnapshot,
      ProviderAdapterSessionNotFoundError | ProviderAdapterValidationError
    > => {
      const ctx = requireSession(threadId);
      if (!ctx) {
        return Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
      }
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          }),
        );
      }
      ctx.turns.splice(Math.max(0, ctx.turns.length - numTurns));
      return Effect.succeed({ threadId, turns: ctx.turns });
    };
    return {
      provider: PROVIDER, capabilities: { sessionModelSwitch: "in-session" }, startSession, sendTurn,
      interruptTurn: (threadId, turnId) => { const ctx = requireSession(threadId); if (!ctx || !ctx.activeTurnId || (turnId && turnId !== ctx.activeTurnId)) return Effect.fail(new ProviderAdapterValidationError({ provider: PROVIDER, operation: "interrupt", issue: "No matching Dora turn is active." })); return command(threadId, "interrupt", { turnId: String(ctx.activeTurnId) }); },
      respondToRequest: (threadId, requestId, decision) => respondToInteraction(threadId, requestId, "approval", { decision }),
      respondToUserInput: (threadId, requestId, answers) => respondToInteraction(threadId, requestId, "input", { answers }),
      stopSession,
      listSessions: () => Effect.sync(() => [...sessions.values()].filter((ctx) => !ctx.stopped).map((ctx) => ctx.session)),
      hasSession: (threadId) => Effect.sync(() => requireSession(threadId) !== undefined),
      readThread: (threadId) => { const ctx = requireSession(threadId); return ctx ? Effect.succeed({ threadId, turns: ctx.turns }) : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId })); },
      rollbackThread,
      stopAll: () => Effect.forEach([...sessions.keys()], stopSession, { discard: true }),
      get streamEvents() { return Stream.fromQueue(runtimeEvents); },
    } satisfies DoraAdapterShape;
  });
}
