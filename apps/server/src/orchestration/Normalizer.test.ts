import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  type ClientOrchestrationCommand,
  EventId,
  MessageId,
  type OrchestrationCommand,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import {
  canonicalizeClientCommandTimestamps,
  normalizeDispatchCommand,
  validateDoraActivity,
} from "./Normalizer.ts";

const clientCreatedAt = "2031-01-01T00:00:00.000Z";
const serverReceivedAt = "2026-07-18T00:00:00.000Z";

const testLayer = Layer.mergeAll(
  WorkspacePaths.layer,
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-normalizer-dora-" }),
).pipe(Layer.provideMerge(NodeServices.layer));

type DoraActivityAppendCommand = Extract<
  ClientOrchestrationCommand,
  { type: "thread.activity.append" }
>;
type NormalizedDoraActivityAppendCommand = Extract<
  OrchestrationCommand,
  {
    type: "thread.activity.append";
    providerInstanceId: ProviderInstanceId;
    providerSessionId: ProviderSessionId;
  }
>;

function activityOf(command: ClientOrchestrationCommand): DoraActivityAppendCommand["activity"] {
  if (command.type !== "thread.activity.append") {
    throw new Error("Expected a Dora activity append command.");
  }
  return command.activity;
}

function isNormalizedDoraActivityAppendCommand(
  command: OrchestrationCommand,
): command is NormalizedDoraActivityAppendCommand {
  return (
    command.type === "thread.activity.append" &&
    "providerInstanceId" in command &&
    "providerSessionId" in command
  );
}

describe("canonicalizeClientCommandTimestamps", () => {
  it("replaces a client command timestamp with the server receipt timestamp", () => {
    const command: ClientOrchestrationCommand = {
      type: "project.create",
      commandId: CommandId.make("command-1"),
      projectId: ProjectId.make("project-1"),
      title: "Clock-safe project",
      workspaceRoot: "/tmp/clock-safe-project",
      createdAt: clientCreatedAt,
    };

    expect(canonicalizeClientCommandTimestamps(command, serverReceivedAt)).toEqual({
      ...command,
      createdAt: serverReceivedAt,
    });
  });

  it("replaces both timestamps when the first turn bootstraps a thread", () => {
    const command: ClientOrchestrationCommand = {
      type: "thread.turn.start",
      commandId: CommandId.make("command-2"),
      threadId: ThreadId.make("thread-1"),
      message: {
        messageId: MessageId.make("message-1"),
        role: "user",
        text: "Start a thread",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      bootstrap: {
        createThread: {
          projectId: ProjectId.make("project-1"),
          title: "Clock-safe thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.4",
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: clientCreatedAt,
        },
      },
      createdAt: clientCreatedAt,
    };

    const result = canonicalizeClientCommandTimestamps(command, serverReceivedAt);

    expect(result.type).toBe("thread.turn.start");
    if (result.type !== "thread.turn.start") {
      throw new Error("Expected a thread.turn.start command");
    }
    expect(result.createdAt).toBe(serverReceivedAt);
    expect(result.bootstrap?.createThread?.createdAt).toBe(serverReceivedAt);
  });

  it("replaces Dora activity timestamps with the server receipt timestamp", () => {
    const command: ClientOrchestrationCommand = {
      type: "thread.activity.append",
      commandId: CommandId.make("command-dora-activity"),
      threadId: ThreadId.make("thread-1"),
      providerInstanceId: ProviderInstanceId.make("dora"),
      providerSessionId: ProviderSessionId.make("dora-session-1"),
      activity: {
        id: EventId.make("activity-dora-1"),
        tone: "info",
        kind: "dora.plan",
        summary: "Created plan",
        payload: { steps: 2 },
        turnId: null,
        createdAt: clientCreatedAt,
      },
      createdAt: clientCreatedAt,
    };

    expect(canonicalizeClientCommandTimestamps(command, serverReceivedAt)).toEqual({
      ...command,
      createdAt: serverReceivedAt,
      activity: {
        ...command.activity,
        createdAt: serverReceivedAt,
      },
    });
  });
});

function doraActivityCommand(payload: unknown): DoraActivityAppendCommand {
  return {
    type: "thread.activity.append",
    commandId: CommandId.make("command-dora-activity"),
    threadId: ThreadId.make("thread-1"),
    providerInstanceId: ProviderInstanceId.make("dora"),
    providerSessionId: ProviderSessionId.make("dora-session-1"),
    activity: {
      id: EventId.make("activity-dora-1"),
      tone: "info",
      kind: "dora.plan",
      summary: "Created plan",
      payload,
      turnId: null,
      createdAt: clientCreatedAt,
    },
    createdAt: clientCreatedAt,
  };
}

describe("normalizeDispatchCommand Dora activities", () => {
  it("normalizes a valid externally dispatched Dora activity", async () => {
    const normalized = await Effect.runPromise(
      normalizeDispatchCommand(doraActivityCommand({ plan: ["inspect", "verify"], attempts: 1 })).pipe(
        Effect.provide(testLayer),
      ),
    );
    if (!isNormalizedDoraActivityAppendCommand(normalized)) {
      throw new Error("Expected a Dora activity append command.");
    }
    expect(normalized.providerInstanceId).toBe("dora");
    expect(normalized.providerSessionId).toBe("dora-session-1");
    expect(normalized.activity.createdAt).not.toBe(clientCreatedAt);
  });

  it("validates recursive bounds and rejects secret-bearing summaries and payloads", () => {
    const validActivity = activityOf(
      doraActivityCommand({ plan: ["inspect", "verify"], attempts: 1 }),
    );
    expect(validateDoraActivity(validActivity)).toBeUndefined();
    expect(
      validateDoraActivity({
        ...validActivity,
        payload: { one: { two: { three: { four: { five: { six: "too deep" } } } } } },
      }),
    ).toContain("maximum depth");
    expect(validateDoraActivity({ ...validActivity, summary: "token: secret-value" })).toContain(
      "secret-bearing",
    );
    expect(
      validateDoraActivity({ ...validActivity, payload: { authorization: "Bearer x" } }),
    ).toContain("secret-bearing");
    expect(
      validateDoraActivity({
        ...validActivity,
        payload: { diagnostics: { detail: "diagnostic: Bearer abcdefghijklmno" } },
      }),
    ).toContain("secret-bearing");
    expect(
      validateDoraActivity({
        ...validActivity,
        payload: { diagnostics: { detail: "Provider response included Bearer abcdefghijklmno in prose." } },
      }),
    ).toContain("secret-bearing");
  });

  it("rejects a deeply nested payload with a typed validation error before secret scanning", async () => {
    const payload: Record<string, unknown> = {};
    let current = payload;
    for (let depth = 0; depth < 10_000; depth += 1) {
      const next: Record<string, unknown> = {};
      current.next = next;
      current = next;
    }

    const error = await Effect.runPromise(
      normalizeDispatchCommand(doraActivityCommand(payload)).pipe(Effect.provide(testLayer), Effect.flip),
    );

    expect(error._tag).toBe("OrchestrationDispatchCommandError");
    expect(error.message).toContain("payload exceeds maximum depth");
  });

  it("rejects non-Dora kinds and unsafe or oversized payloads", async () => {
    const baseCommand = doraActivityCommand({});
    const baseActivity = activityOf(baseCommand);
    const invalidCommands: ClientOrchestrationCommand[] = [
      ...["tool.completed", "approval.requested", "user-input.requested", "other.kind"].map(
        (kind) =>
          ({
            ...baseCommand,
            activity: { ...baseActivity, kind },
          }) as unknown as ClientOrchestrationCommand,
      ),
      {
        ...baseCommand,
        activity: { ...baseActivity, tone: "approval" },
      } as unknown as ClientOrchestrationCommand,
      {
        ...baseCommand,
        activity: { ...baseActivity, turnId: "turn-1" },
      } as unknown as ClientOrchestrationCommand,
      {
        ...baseCommand,
        activity: { ...baseActivity, summary: "x".repeat(2_001) },
      } as unknown as ClientOrchestrationCommand,
      doraActivityCommand(["not-a-record"]),
      doraActivityCommand({ detail: "x".repeat(4_097) }),
      doraActivityCommand({
        one: "x".repeat(4_096),
        two: "x".repeat(4_096),
        three: "x".repeat(4_096),
        four: "x".repeat(4_096),
        five: "x".repeat(4_096),
      }),
      doraActivityCommand(JSON.parse('{"__proto__":"unsafe"}')),
      doraActivityCommand({ count: Number.NaN }),
    ];

    for (const command of invalidCommands) {
      const error = await Effect.runPromise(
        normalizeDispatchCommand(command).pipe(Effect.provide(testLayer), Effect.flip),
      );
      expect(error._tag).toBe("OrchestrationDispatchCommandError");
      expect(error.message).toContain("Invalid Dora activity:");
    }
  });
});
