// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ApprovalRequestId,
  EventId,
  CheckpointRef,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type OrchestrationThread,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadShell,
  ProviderInstanceId,
  ProviderSessionId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { PersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import * as OrchestrationCommandReceipts from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import {
  OrchestrationEventStore,
  type OrchestrationEventStoreShape,
} from "../../persistence/Services/OrchestrationEventStore.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  OrchestrationProjectionPipeline,
  type OrchestrationProjectionPipelineShape,
} from "../Services/ProjectionPipeline.ts";
import {
  ProjectionSnapshotQuery,
  type ProjectionSnapshotQueryShape,
} from "../Services/ProjectionSnapshotQuery.ts";
import { ServerConfig } from "../../config.ts";
import { createAuthenticatedDoraActivityCapability } from "../DoraActivityAuthorization.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asCheckpointRef = (value: string): CheckpointRef => CheckpointRef.make(value);

function makeOrchestrationLayer(databasePath?: string) {
  const persistence = databasePath
    ? makeSqlitePersistenceLive(databasePath)
    : SqlitePersistenceMemory;
  const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-orchestration-engine-test-",
  });
  return Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(persistence),
    Layer.provideMerge(ServerConfigLayer),
    Layer.provideMerge(NodeServices.layer),
  );
}

async function createOrchestrationSystem(databasePath?: string) {
  const runtime = ManagedRuntime.make(makeOrchestrationLayer(databasePath));
  const sqlRuntime =
    databasePath === undefined
      ? undefined
      : ManagedRuntime.make(
          makeSqlitePersistenceLive(databasePath).pipe(Layer.provideMerge(NodeServices.layer)),
        );
  const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
  const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
  return {
    engine,
    readModel: () => runtime.runPromise(snapshotQuery.getSnapshot()),
    commandReadModel: () => runtime.runPromise(snapshotQuery.getCommandReadModel()),
    readThread: (threadId: ThreadId) =>
      runtime.runPromise(snapshotQuery.getThreadDetailById(threadId)),
    backgroundLiveness: () =>
      runtime.runPromise(Effect.service(ThreadBackgroundLiveness.ThreadBackgroundLivenessService)),
    run: <A, E>(effect: Effect.Effect<A, E>) => runtime.runPromise(effect),
    sql: () => {
      if (sqlRuntime === undefined) {
        throw new Error("A database path is required for test SQL access.");
      }
      return sqlRuntime.runPromise(Effect.service(SqlClient.SqlClient));
    },
    runSql: <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) => {
      if (sqlRuntime === undefined) {
        throw new Error("A database path is required for test SQL access.");
      }
      return sqlRuntime.runPromise(effect);
    },
    dispose: async () => {
      await runtime.dispose();
      await sqlRuntime?.dispose();
    },
  };
}

function now() {
  return "2026-01-01T00:00:00.000Z";
}

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

function makeDoraCommandReadModel(
  snapshotSequence: number,
  session: "absent" | "bound",
): OrchestrationReadModel {
  const projectId = ProjectId.make("project-dora-reconciliation");
  const threadId = ThreadId.make("thread-dora-reconciliation");
  const providerInstanceId = ProviderInstanceId.make("dora");
  const providerSessionId = ProviderSessionId.make("dora-session-reconciliation");
  const createdAt = now();

  return {
    snapshotSequence,
    updatedAt: createdAt,
    projects: [
      {
        id: projectId,
        title: "Dora reconciliation",
        workspaceRoot: "/tmp/dora-reconciliation",
        defaultModelSelection: null,
        scripts: [],
        createdAt,
        updatedAt: createdAt,
        deletedAt: null,
      },
    ],
    threads: [
      {
        id: threadId,
        projectId,
        title: "Dora reconciliation",
        modelSelection: { instanceId: providerInstanceId, model: "dora" },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session:
          session === "bound"
            ? {
                threadId,
                status: "running",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "full-access",
                activeTurnId: TurnId.make("turn-dora-reconciliation"),
                lastError: null,
                updatedAt: createdAt,
              }
            : null,
      },
    ],
  };
}

function makeStoppedDoraReadModel(snapshotSequence: number): OrchestrationReadModel {
  const base = makeDoraCommandReadModel(snapshotSequence, "absent");
  const threadId = ThreadId.make("thread-dora-reconciliation");
  const providerInstanceId = ProviderInstanceId.make("dora");
  const providerSessionId = ProviderSessionId.make("dora-session-reconciliation");
  const turnId = TurnId.make("turn-dora-reconciliation");
  const assistantMessageId = MessageId.make("assistant-dora-reconciliation");
  const completedAt = "2026-01-01T00:00:01.000Z";

  return {
    ...base,
    threads: [
      {
        ...base.threads[0]!,
        latestTurn: {
          turnId,
          state: "completed",
          requestedAt: now(),
          startedAt: now(),
          completedAt,
          assistantMessageId,
          requestMessageId: null,
        },
        messages: [
          {
            id: assistantMessageId,
            role: "assistant",
            text: "completed",
            turnId,
            streaming: false,
            createdAt: completedAt,
            updatedAt: completedAt,
          },
        ],
        session: {
          threadId,
          status: "stopped",
          providerName: "dora",
          providerInstanceId,
          providerSessionId,
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: completedAt,
        },
      },
    ],
  };
}

function makeStoppedDoraShell(thread: OrchestrationThread): OrchestrationThreadShell {
  return {
    ...thread,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    backgroundLiveness: null,
    planProgress: null,
  };
}

function makeDoraReconciliationRuntime(
  commandReadModels: ReadonlyArray<OrchestrationReadModel>,
  options?: {
    readonly reconciliationReadError?: PersistenceSqlError;
    readonly threadDetailSnapshot?: Option.Option<OrchestrationThreadDetailSnapshot>;
    readonly threadDetailSnapshotError?: PersistenceSqlError;
    readonly threadShell?: Option.Option<OrchestrationThreadShell>;
    readonly threadShellError?: PersistenceSqlError;
  },
) {
  let commandReadModelReads = 0;
  let appendCount = 0;
  let nextSequence = (commandReadModels.at(-1)?.snapshotSequence ?? 0) + 1;
  const commandReadModel = () =>
    Effect.suspend(() => {
      const readIndex = commandReadModelReads;
      commandReadModelReads += 1;
      if (readIndex > 0 && options?.reconciliationReadError !== undefined) {
        return Effect.fail(options.reconciliationReadError);
      }
      const readModel = commandReadModels.at(Math.min(readIndex, commandReadModels.length - 1));
      return readModel === undefined
        ? Effect.die("A command read model is required for this test.")
        : Effect.succeed(readModel);
    });
  const snapshotQuery: ProjectionSnapshotQueryShape = {
    getCommandReadModel: commandReadModel,
    getUserInputActivity: () => Effect.die("unused"),
    getSnapshot: () => Effect.die("unused"),
    getShellSnapshot: () => Effect.die("unused"),
    getArchivedShellSnapshot: () => Effect.die("unused"),
    searchThreads: () => Effect.die("unused"),
    getSnapshotSequence: () => Effect.die("unused"),
    getCounts: () => Effect.die("unused"),
    getEventReplayStats: () => Effect.die("unused"),
    getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
    getProjectShellById: () => Effect.die("unused"),
    getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
    getThreadCheckpointContext: () => Effect.die("unused"),
    getFullThreadDiffContext: () => Effect.die("unused"),
    getThreadShellById: () =>
      options?.threadShellError !== undefined
        ? Effect.fail(options.threadShellError)
        : options?.threadShell === undefined
          ? Effect.die("unused")
          : Effect.succeed(options.threadShell),
    getThreadDetailById: () => Effect.die("unused"),
    getThreadDetailSnapshot: () =>
      options?.threadDetailSnapshotError !== undefined
        ? Effect.fail(options.threadDetailSnapshotError)
        : options?.threadDetailSnapshot === undefined
          ? Effect.die("unused")
          : Effect.succeed(options.threadDetailSnapshot),
  };
  const eventStore: OrchestrationEventStoreShape = {
    append: (event) =>
      Effect.sync(() => {
        appendCount += 1;
        const savedEvent = { ...event, sequence: nextSequence } as OrchestrationEvent;
        nextSequence += 1;
        return savedEvent;
      }),
    readFromSequence: () => Stream.empty,
    readAll: () => Stream.empty,
    hasEventAfter: () => Effect.succeed(false),
  };
  const runtime = ManagedRuntime.make(
    OrchestrationEngineLive.pipe(
      Layer.provide(Layer.succeed(ProjectionSnapshotQuery, snapshotQuery)),
      Layer.provide(
        Layer.succeed(OrchestrationProjectionPipeline, {
          bootstrap: Effect.void,
          projectEvent: () => Effect.void,
          projectEventDeferred: () => Effect.succeed(Effect.void),
        } satisfies OrchestrationProjectionPipelineShape),
      ),
      Layer.provide(Layer.succeed(OrchestrationEventStore, eventStore)),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    ),
  );

  return {
    runtime,
    commandReadModelReads: () => commandReadModelReads,
    appendCount: () => appendCount,
  };
}

describe("OrchestrationEngine", () => {
  it.each(["running", "stopped"] as const)(
    "sends async answers with a %s session and rejects old duplicate replies",
    async (status) => {
      const directory = await NodeFSP.mkdtemp(
        NodePath.join(NodeOS.tmpdir(), "t3-async-questions-"),
      );
      const databasePath = NodePath.join(directory, "state.sqlite");
      let system = await createOrchestrationSystem(databasePath);
      const threadId = ThreadId.make("async-thread");
      const projectId = ProjectId.make("async-project");
      const requestId = ApprovalRequestId.make("codex-async:question-1");
      try {
        await system.run(
          system.engine.dispatch({
            type: "project.create",
            commandId: CommandId.make("async-project"),
            projectId,
            title: "Async questions",
            workspaceRoot: "/tmp/async-questions",
            createdAt: now(),
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("async-thread"),
            threadId,
            projectId,
            title: "Async questions",
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: now(),
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("async-session"),
            threadId,
            createdAt: now(),
            session: {
              threadId,
              status,
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: status === "running" ? TurnId.make("turn-1") : null,
              lastError: null,
              updatedAt: now(),
            },
          }),
        );
        await system.run(
          system.engine.dispatch({
            type: "thread.activity.append",
            commandId: CommandId.make("async-question"),
            threadId,
            createdAt: now(),
            activity: {
              id: EventId.make("async-question"),
              kind: "user-input.requested",
              summary: "User input requested",
              tone: "info",
              turnId: TurnId.make("turn-1"),
              createdAt: now(),
              payload: {
                requestId,
                responseMode: "message",
                questions: [
                  {
                    id: "0",
                    header: "Question",
                    question: "Which package manager?",
                    options: [{ label: "pnpm", description: "" }],
                  },
                  {
                    id: "1",
                    header: "Question",
                    question: "What should it be named?",
                    options: [],
                  },
                ],
              },
            },
          }),
        );
        const appendWork = async (prefix: string, createdAt: string) => {
          for (let index = 0; index < 501; index += 1) {
            await system.run(
              system.engine.dispatch({
                type: "thread.activity.append",
                commandId: CommandId.make(`${prefix}-${index}`),
                threadId,
                createdAt,
                activity: {
                  id: EventId.make(`${prefix}-${index}`),
                  kind: "tool.completed",
                  summary: "Work continued",
                  payload: {},
                  tone: "info",
                  turnId: TurnId.make("turn-1"),
                  createdAt,
                },
              }),
            );
          }
        };
        await appendWork("work", "2026-01-01T00:00:01.000Z");
        const before = await system.readModel();
        expect(
          before.threads[0]?.activities.some((activity) => activity.id === "async-question"),
        ).toBe(true);
        if (status === "stopped") {
          await system.dispose();
          system = await createOrchestrationSystem(databasePath);
        }
        const response = {
          type: "thread.user-input.respond" as const,
          commandId: CommandId.make("async-response"),
          threadId,
          requestId,
          answers: { "0": "pnpm", "1": "Example" },
          createdAt: "2026-01-01T00:00:02.000Z",
        };
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("incomplete-answer"),
              answers: { "0": "pnpm" },
            }),
          ),
        ).rejects.toThrow("Answer each question before sending.");
        await system.run(system.engine.dispatch(response));
        const after = await system.readModel();
        const userMessages = after.threads[0]?.messages.filter(
          (message) => message.role === "user",
        );
        expect(userMessages).toHaveLength(1);
        expect(userMessages?.[0]?.text).toBe(
          "Which package manager?\npnpm\n\nWhat should it be named?\nExample",
        );
        expect(
          after.threads[0]?.activities.find((activity) => activity.kind === "user-input.resolved")
            ?.payload,
        ).toMatchObject({ requestId, responseMode: "message", answers: response.answers });
        const events = await system.run(Stream.runCollect(system.engine.readEvents(0)));
        expect(
          Array.from(events)
            .filter((event) => event.commandId === response.commandId)
            .map((event) => event.type),
        ).toEqual([
          "thread.activity-appended",
          "thread.message-sent",
          "thread.turn-start-requested",
        ]);
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("second-client-reply"),
            }),
          ),
        ).rejects.toThrow("This question has already been answered.");
        await appendWork("later-work", "2026-01-01T00:00:03.000Z");
        const afterEviction = Option.getOrThrow(await system.readThread(threadId));
        expect(
          afterEviction.activities.some((activity) => activity.kind === "user-input.resolved"),
        ).toBe(false);
        if (status === "stopped") {
          await system.dispose();
          system = await createOrchestrationSystem(databasePath);
        }
        await expect(
          system.run(
            system.engine.dispatch({
              ...response,
              commandId: CommandId.make("reply-after-eviction"),
            }),
          ),
        ).rejects.toThrow("This question has already been answered.");
      } finally {
        await system.dispose();
        await NodeFSP.rm(directory, { recursive: true, force: true });
      }
    },
  );

  it("bootstraps command handling from persisted projections without reading the full snapshot", async () => {
    let nextSequence = 8;
    const eventStore: OrchestrationEventStoreShape = {
      append: (event) =>
        Effect.sync(() => {
          const savedEvent = {
            ...event,
            sequence: nextSequence,
          } as OrchestrationEvent;
          nextSequence += 1;
          return savedEvent;
        }),
      readFromSequence: () => Stream.empty,
      readAll: () =>
        Stream.fail(
          new PersistenceSqlError({
            operation: "test.readAll",
            detail: "historical replay should not be used during bootstrap",
          }),
        ),
      hasEventAfter: () => Effect.succeed(false),
    };

    const projectionSnapshot = {
      snapshotSequence: 7,
      updatedAt: "2026-03-03T00:00:04.000Z",
      projects: [
        {
          id: asProjectId("project-bootstrap"),
          title: "Bootstrap Project",
          workspaceRoot: "/tmp/project-bootstrap",
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          scripts: [],
          createdAt: "2026-03-03T00:00:00.000Z",
          updatedAt: "2026-03-03T00:00:01.000Z",
          deletedAt: null,
        },
      ],
      threads: [
        {
          id: ThreadId.make("thread-bootstrap"),
          projectId: asProjectId("project-bootstrap"),
          title: "Bootstrap Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access" as const,
          branch: null,
          worktreePath: null,
          latestTurn: null,
          createdAt: "2026-03-03T00:00:02.000Z",
          updatedAt: "2026-03-03T00:00:03.000Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          deletedAt: null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: null,
        },
      ],
    };
    const commandReadModel = {
      ...projectionSnapshot,
      threads: projectionSnapshot.threads.map((thread) => ({
        ...thread,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
      })),
    };
    let fullSnapshotReadCount = 0;

    const layer = OrchestrationEngineLive.pipe(
      Layer.provide(
        Layer.succeed(ProjectionSnapshotQuery, {
          getUserInputActivity: () => Effect.die("unused"),
          getCommandReadModel: () => Effect.succeed(commandReadModel),
          getSnapshot: () =>
            Effect.sync(() => {
              fullSnapshotReadCount += 1;
              return projectionSnapshot;
            }),
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getArchivedShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: projectionSnapshot.snapshotSequence,
              projects: [],
              threads: [],
              updatedAt: projectionSnapshot.updatedAt,
            }),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: projectionSnapshot.snapshotSequence }),
          getCounts: () => Effect.succeed({ projectCount: 1, threadCount: 1 }),
          getEventReplayStats: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
          getProjectShellById: () => Effect.succeed(Option.none()),
          getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          getFullThreadDiffContext: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.succeed(Option.none()),
          getThreadDetailById: () => Effect.succeed(Option.none()),
          getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
      ),
      Layer.provide(
        Layer.succeed(OrchestrationProjectionPipeline, {
          bootstrap: Effect.void,
          projectEvent: () => Effect.void,
          projectEventDeferred: () => Effect.succeed(Effect.void),
        } satisfies OrchestrationProjectionPipelineShape),
      ),
      Layer.provide(Layer.succeed(OrchestrationEventStore, eventStore)),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    );

    const runtime = ManagedRuntime.make(layer);

    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    expect(await runtime.runPromise(engine.latestSequence)).toBe(7);
    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-bootstrap-thread-update"),
        threadId: ThreadId.make("thread-bootstrap"),
        title: "Updated Bootstrap Thread",
      }),
    );

    expect(result.sequence).toBe(8);
    expect(await runtime.runPromise(engine.latestSequence)).toBe(8);
    expect(fullSnapshotReadCount).toBe(0);

    await runtime.dispose();
  });

  effectIt.effect(
    "reconciles non-stale Dora session bindings and rejects reconciliation failures",
    () =>
      Effect.promise(async () => {
        const threadId = ThreadId.make("thread-dora-reconciliation");
        const providerInstanceId = ProviderInstanceId.make("dora");
        const providerSessionId = ProviderSessionId.make("dora-session-reconciliation");
        const capability = createAuthenticatedDoraActivityCapability({
          threadId,
          providerInstanceId,
          providerSessionId,
        });
        const activity = (commandId: CommandId) => ({
          type: "thread.activity.append" as const,
          commandId,
          threadId,
          providerInstanceId,
          providerSessionId,
          activity: {
            id: EventId.make(`activity-${commandId}`),
            tone: "info" as const,
            kind: "dora.plan" as const,
            summary: "Plan completed",
            payload: {},
            turnId: null,
            createdAt: now(),
          },
          createdAt: now(),
        });

        const reconciled = makeDoraReconciliationRuntime([
          makeDoraCommandReadModel(7, "absent"),
          makeDoraCommandReadModel(8, "bound"),
        ]);
        try {
          const engine = await reconciled.runtime.runPromise(
            Effect.service(OrchestrationEngineService),
          );
          const accepted = await reconciled.runtime.runPromise(
            engine.dispatch(activity(CommandId.make("cmd-dora-reconciliation-accepted")), {
              doraActivityCapability: capability,
            }),
          );
          expect(accepted.sequence).toBe(9);
          expect(await reconciled.runtime.runPromise(engine.latestSequence)).toBe(9);
          expect(reconciled.commandReadModelReads()).toBe(2);
          expect(reconciled.appendCount()).toBe(1);
        } finally {
          await reconciled.runtime.dispose();
        }

        const equalSequence = makeDoraReconciliationRuntime([
          makeDoraCommandReadModel(7, "absent"),
          makeDoraCommandReadModel(7, "bound"),
        ]);
        try {
          const engine = await equalSequence.runtime.runPromise(
            Effect.service(OrchestrationEngineService),
          );
          const accepted = await equalSequence.runtime.runPromise(
            engine.dispatch(activity(CommandId.make("cmd-dora-reconciliation-equal")), {
              doraActivityCapability: capability,
            }),
          );
          expect(accepted.sequence).toBe(8);
          expect(await equalSequence.runtime.runPromise(engine.latestSequence)).toBe(8);
          expect(equalSequence.commandReadModelReads()).toBe(2);
          expect(equalSequence.appendCount()).toBe(1);
        } finally {
          await equalSequence.runtime.dispose();
        }

        const stale = makeDoraReconciliationRuntime([
          makeDoraCommandReadModel(7, "absent"),
          makeDoraCommandReadModel(6, "bound"),
        ]);
        try {
          const engine = await stale.runtime.runPromise(Effect.service(OrchestrationEngineService));
          const commandId = CommandId.make("cmd-dora-reconciliation-stale");
          await expect(
            stale.runtime.runPromise(
              engine.dispatch(activity(commandId), { doraActivityCapability: capability }),
            ),
          ).rejects.toThrow("Dora activity does not match an active bound Dora session.");
          const receipts = await stale.runtime.runPromise(
            Effect.service(OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository),
          );
          expect(
            Option.getOrNull(
              await stale.runtime.runPromise(receipts.getByCommandId({ commandId })),
            ),
          ).toMatchObject({
            commandId,
            aggregateKind: "thread",
            aggregateId: threadId,
            status: "rejected",
            resultSequence: 7,
          });
          expect(await stale.runtime.runPromise(engine.latestSequence)).toBe(7);
          expect(stale.commandReadModelReads()).toBe(2);
          expect(stale.appendCount()).toBe(0);
        } finally {
          await stale.runtime.dispose();
        }

        const readFailure = makeDoraReconciliationRuntime([makeDoraCommandReadModel(7, "absent")], {
          reconciliationReadError: new PersistenceSqlError({
            operation: "test.dora-reconciliation-read",
            detail: "projection unavailable",
          }),
        });
        try {
          const engine = await readFailure.runtime.runPromise(
            Effect.service(OrchestrationEngineService),
          );
          const commandId = CommandId.make("cmd-dora-reconciliation-read-failure");
          await expect(
            readFailure.runtime.runPromise(
              engine.dispatch(activity(commandId), { doraActivityCapability: capability }),
            ),
          ).rejects.toThrow("Dora activity does not match an active bound Dora session.");
          const receipts = await readFailure.runtime.runPromise(
            Effect.service(OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository),
          );
          expect(
            Option.getOrNull(
              await readFailure.runtime.runPromise(receipts.getByCommandId({ commandId })),
            ),
          ).toMatchObject({
            commandId,
            aggregateKind: "thread",
            aggregateId: threadId,
            status: "rejected",
            error:
              "Orchestration command invariant failed (thread.activity.append): Dora activity does not match an active bound Dora session.",
            resultSequence: 7,
          });
          expect(await readFailure.runtime.runPromise(engine.latestSequence)).toBe(7);
          expect(readFailure.commandReadModelReads()).toBe(2);
          expect(readFailure.appendCount()).toBe(0);
        } finally {
          await readFailure.runtime.dispose();
        }
      }),
  );

  effectIt.effect("binds stopped Dora closure evidence to the current command model", () =>
    Effect.promise(async () => {
      const current = makeStoppedDoraReadModel(8);
      const thread = current.threads[0]!;
      const capability = createAuthenticatedDoraActivityCapability({
        threadId: thread.id,
        providerInstanceId: ProviderInstanceId.make("dora"),
        providerSessionId: ProviderSessionId.make("dora-session-reconciliation"),
      });
      const command = (commandId: string) => ({
        type: "thread.activity.append" as const,
        commandId: CommandId.make(commandId),
        threadId: thread.id,
        providerInstanceId: ProviderInstanceId.make("dora"),
        providerSessionId: ProviderSessionId.make("dora-session-reconciliation"),
        activity: {
          id: EventId.make(`activity-${commandId}`),
          tone: "info" as const,
          kind: "dora.verification" as const,
          summary: "Verified",
          payload: {},
          turnId: null,
          createdAt: now(),
        },
        createdAt: now(),
      });
      const shell = makeStoppedDoraShell(thread);
      const cases: ReadonlyArray<{
        readonly name: string;
        readonly detail: Option.Option<OrchestrationThreadDetailSnapshot>;
        readonly shell: Option.Option<OrchestrationThreadShell>;
        readonly accepted: boolean;
      }> = [
        {
          name: "accepts unrelated higher projection sequence",
          detail: Option.some({ snapshotSequence: 9, thread }),
          shell: Option.some(shell),
          accepted: true,
        },
        {
          name: "rejects a lower projection sequence",
          detail: Option.some({ snapshotSequence: 7, thread }),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects missing detail",
          detail: Option.none(),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects missing shell",
          detail: Option.some({ snapshotSequence: 9, thread }),
          shell: Option.none(),
          accepted: false,
        },
        {
          name: "rejects a foreign assistant proof",
          detail: Option.some({
            snapshotSequence: 9,
            thread: {
              ...thread,
              latestTurn: { ...thread.latestTurn!, assistantMessageId: MessageId.make("foreign") },
            },
          }),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects missing assistant proof",
          detail: Option.some({ snapshotSequence: 9, thread: { ...thread, messages: [] } }),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects malformed completion time",
          detail: Option.some({
            snapshotSequence: 9,
            thread: { ...thread, latestTurn: { ...thread.latestTurn!, completedAt: "invalid" } },
          }),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects a later user message",
          detail: Option.some({
            snapshotSequence: 9,
            thread: {
              ...thread,
              messages: [
                ...thread.messages,
                {
                  id: MessageId.make("later-user"),
                  role: "user",
                  text: "new work",
                  turnId: null,
                  streaming: false,
                  createdAt: "2026-01-01T00:00:02.000Z",
                  updatedAt: "2026-01-01T00:00:02.000Z",
                },
              ],
            },
          }),
          shell: Option.some(shell),
          accepted: false,
        },
        {
          name: "rejects pending approval",
          detail: Option.some({ snapshotSequence: 9, thread }),
          shell: Option.some({ ...shell, hasPendingApprovals: true }),
          accepted: false,
        },
      ];

      for (const testCase of cases) {
        const runtime = makeDoraReconciliationRuntime([current], {
          threadDetailSnapshot: testCase.detail,
          threadShell: testCase.shell,
        });
        try {
          const engine = await runtime.runtime.runPromise(
            Effect.service(OrchestrationEngineService),
          );
          if (testCase.accepted) {
            await runtime.runtime.runPromise(
              engine.dispatch(command(`cmd-stopped-evidence-${testCase.name}`), {
                doraActivityCapability: capability,
              }),
            );
            expect(runtime.appendCount()).toBe(1);
          } else {
            await expect(
              runtime.runtime.runPromise(
                engine.dispatch(command(`cmd-stopped-evidence-${testCase.name}`), {
                  doraActivityCapability: capability,
                }),
              ),
            ).rejects.toThrow("clean completed stopped Dora session");
            expect(runtime.appendCount()).toBe(0);
          }
        } finally {
          await runtime.runtime.dispose();
        }
      }
    }),
  );

  effectIt.effect(
    "rejects every remaining stopped Dora closure proof mismatch without appending",
    () =>
      Effect.promise(async () => {
        const current = makeStoppedDoraReadModel(8);
        const thread = current.threads[0]!;
        const capability = createAuthenticatedDoraActivityCapability({
          threadId: thread.id,
          providerInstanceId: ProviderInstanceId.make("dora"),
          providerSessionId: ProviderSessionId.make("dora-session-reconciliation"),
        });
        const command = (
          commandId: string,
          kind:
            | "dora.plan"
            | "dora.replan"
            | "dora.verification"
            | "dora.side-effect" = "dora.verification",
          payload: Record<string, unknown> = {},
        ) => ({
          type: "thread.activity.append" as const,
          commandId: CommandId.make(commandId),
          threadId: thread.id,
          providerInstanceId: ProviderInstanceId.make("dora"),
          providerSessionId: ProviderSessionId.make("dora-session-reconciliation"),
          activity: {
            id: EventId.make(`activity-${commandId}`),
            tone: "info" as const,
            kind,
            summary: "Verified",
            payload,
            turnId: null,
            createdAt: now(),
          },
          createdAt: now(),
        });
        const all = (nextThread: OrchestrationThread, snapshotSequence = 9) => ({
          current: { ...current, threads: [nextThread] },
          detail: Option.some({ snapshotSequence, thread: nextThread }),
          shell: Option.some(makeStoppedDoraShell(nextThread)),
        });
        const detailOnly = (nextThread: OrchestrationThread, snapshotSequence = 9) => ({
          current,
          detail: Option.some({ snapshotSequence, thread: nextThread }),
          shell: Option.some(makeStoppedDoraShell(nextThread)),
        });
        const withoutProviderSessionId = (candidate: OrchestrationThread): OrchestrationThread => {
          const { providerSessionId: _providerSessionId, ...session } = candidate.session!;
          return { ...candidate, session };
        };
        const withLatestTurn = (
          candidate: OrchestrationThread,
          turn: NonNullable<OrchestrationThread["latestTurn"]>,
          messages = candidate.messages,
        ): OrchestrationThread => ({ ...candidate, latestTurn: turn, messages });
        const cases: ReadonlyArray<{
          readonly name: string;
          readonly current: OrchestrationReadModel;
          readonly detail?: Option.Option<OrchestrationThreadDetailSnapshot>;
          readonly shell?: Option.Option<OrchestrationThreadShell>;
          readonly threadDetailSnapshotError?: PersistenceSqlError;
          readonly threadShellError?: PersistenceSqlError;
          readonly kind?: "dora.plan" | "dora.replan" | "dora.verification" | "dora.side-effect";
          readonly payload?: Record<string, unknown>;
          readonly accepted?: boolean;
        }> = [
          {
            name: "accepts ready current binding despite stale stopped detail",
            current: {
              ...current,
              threads: [{ ...thread, session: { ...thread.session!, status: "ready" } }],
            },
            detail: Option.some({ snapshotSequence: 9, thread }),
            shell: Option.some(makeStoppedDoraShell(thread)),
            accepted: true,
          },
          {
            name: "accepts running current binding despite stale stopped detail",
            current: makeDoraCommandReadModel(8, "bound"),
            detail: Option.some({ snapshotSequence: 9, thread }),
            shell: Option.some(makeStoppedDoraShell(thread)),
            accepted: true,
          },
          {
            name: "rejects missing explicit provider session identity",
            ...all(withoutProviderSessionId(thread)),
          },
          {
            name: "rejects foreign provider session identity",
            ...all({
              ...thread,
              session: {
                ...thread.session!,
                providerSessionId: ProviderSessionId.make("foreign-dora-session"),
              },
            }),
          },
          {
            name: "rejects foreign provider instance identity",
            ...all({
              ...thread,
              session: {
                ...thread.session!,
                providerInstanceId: ProviderInstanceId.make("foreign-dora"),
              },
            }),
          },
          {
            name: "rejects foreign provider name",
            ...all({ ...thread, session: { ...thread.session!, providerName: "codex" } }),
          },
          {
            name: "rejects foreign detail thread",
            ...detailOnly({
              ...thread,
              id: ThreadId.make("foreign-dora-thread"),
              session: { ...thread.session!, threadId: ThreadId.make("foreign-dora-thread") },
            }),
          },
          {
            name: "rejects foreign detail project",
            ...detailOnly({ ...thread, projectId: ProjectId.make("foreign-dora-project") }),
          },
          {
            name: "rejects an archived current thread",
            current: {
              ...current,
              threads: [{ ...thread, archivedAt: "2026-01-01T00:00:02.000Z" }],
            },
            detail: Option.some({ snapshotSequence: 9, thread }),
            shell: Option.some(makeStoppedDoraShell(thread)),
          },
          {
            name: "rejects an archived detail thread",
            ...detailOnly({ ...thread, archivedAt: "2026-01-01T00:00:02.000Z" }),
          },
          {
            name: "rejects a deleted current thread",
            current: {
              ...current,
              threads: [{ ...thread, deletedAt: "2026-01-01T00:00:02.000Z" }],
            },
            detail: Option.some({ snapshotSequence: 9, thread }),
            shell: Option.some(makeStoppedDoraShell(thread)),
          },
          {
            name: "rejects a deleted detail thread",
            ...detailOnly({ ...thread, deletedAt: "2026-01-01T00:00:02.000Z" }),
          },
          {
            name: "rejects an exact shell turn mismatch",
            current,
            detail: Option.some({ snapshotSequence: 9, thread }),
            shell: Option.some({
              ...makeStoppedDoraShell(thread),
              latestTurn: { ...thread.latestTurn!, turnId: TurnId.make("shell-different-turn") },
            }),
          },
          {
            name: "rejects an active turn",
            ...all({
              ...thread,
              session: { ...thread.session!, activeTurnId: TurnId.make("active-dora-turn") },
            }),
          },
          {
            name: "rejects a terminal error",
            ...all({ ...thread, session: { ...thread.session!, lastError: "provider failed" } }),
          },
          {
            name: "rejects a starting session",
            ...all({ ...thread, session: { ...thread.session!, status: "starting" } }),
          },
          {
            name: "rejects an error session",
            ...all({ ...thread, session: { ...thread.session!, status: "error" } }),
          },
          {
            name: "rejects a newer contradictory current turn",
            current: {
              ...current,
              snapshotSequence: 10,
              threads: [
                withLatestTurn(thread, {
                  ...thread.latestTurn!,
                  turnId: TurnId.make("current-newer"),
                }),
              ],
            },
            detail: Option.some({ snapshotSequence: 10, thread }),
            shell: Option.some(makeStoppedDoraShell(thread)),
          },
          {
            name: "rejects a missing latest turn",
            ...all({ ...thread, latestTurn: null }),
          },
          {
            name: "rejects an interrupted latest turn",
            ...all(withLatestTurn(thread, { ...thread.latestTurn!, state: "interrupted" })),
          },
          {
            name: "rejects an errored latest turn",
            ...all(withLatestTurn(thread, { ...thread.latestTurn!, state: "error" })),
          },
          {
            name: "rejects a different detail turn",
            ...detailOnly(
              withLatestTurn(
                thread,
                { ...thread.latestTurn!, turnId: TurnId.make("foreign-completed-turn") },
                [{ ...thread.messages[0]!, turnId: TurnId.make("foreign-completed-turn") }],
              ),
            ),
          },
          {
            name: "rejects a missing assistant id",
            ...all(withLatestTurn(thread, { ...thread.latestTurn!, assistantMessageId: null })),
          },
          {
            name: "rejects a different assistant id",
            ...detailOnly(
              withLatestTurn(
                thread,
                { ...thread.latestTurn!, assistantMessageId: MessageId.make("foreign-assistant") },
                [{ ...thread.messages[0]!, id: MessageId.make("foreign-assistant") }],
              ),
            ),
          },
          {
            name: "rejects a null completed time",
            ...all(withLatestTurn(thread, { ...thread.latestTurn!, completedAt: null })),
          },
          {
            name: "rejects an invalid completed time consistently",
            ...all(withLatestTurn(thread, { ...thread.latestTurn!, completedAt: "invalid" })),
          },
          {
            name: "rejects duplicate matching terminal assistants",
            ...all({ ...thread, messages: [...thread.messages, { ...thread.messages[0]! }] }),
          },
          {
            name: "rejects a non-assistant terminal message",
            ...all({ ...thread, messages: [{ ...thread.messages[0]!, role: "system" }] }),
          },
          {
            name: "rejects a terminal assistant from another turn",
            ...all({
              ...thread,
              messages: [{ ...thread.messages[0]!, turnId: TurnId.make("other-assistant-turn") }],
            }),
          },
          {
            name: "rejects a streaming terminal assistant",
            ...all({ ...thread, messages: [{ ...thread.messages[0]!, streaming: true }] }),
          },
          {
            name: "rejects whitespace-only terminal assistant text",
            ...all({ ...thread, messages: [{ ...thread.messages[0]!, text: "   " }] }),
          },
          {
            name: "rejects pending user input",
            ...all(thread),
            shell: Option.some({ ...makeStoppedDoraShell(thread), hasPendingUserInput: true }),
          },
          {
            name: "rejects a negative detail sequence",
            ...all(thread, -1),
          },
          {
            name: "rejects a non-safe detail sequence",
            ...all(thread, Number.MAX_SAFE_INTEGER + 1),
          },
          {
            name: "rejects an unavailable detail query",
            current,
            threadDetailSnapshotError: new PersistenceSqlError({
              operation: "test.dora-stopped-detail",
              detail: "detail unavailable",
            }),
            shell: Option.some(makeStoppedDoraShell(thread)),
          },
          {
            name: "rejects an unavailable shell query",
            current,
            detail: Option.some({ snapshotSequence: 9, thread }),
            threadShellError: new PersistenceSqlError({
              operation: "test.dora-stopped-shell",
              detail: "shell unavailable",
            }),
          },
          {
            name: "rejects stopped dora plan",
            ...all(thread),
            kind: "dora.plan",
          },
          {
            name: "rejects stopped dora replan",
            ...all(thread),
            kind: "dora.replan",
          },
          {
            name: "rejects stopped time-gate comment",
            ...all(thread),
            kind: "dora.side-effect",
            payload: { kind: "time-gate-comment" },
          },
          {
            name: "rejects stopped follow-up side effect",
            ...all(thread),
            kind: "dora.side-effect",
            payload: { kind: "follow-up" },
          },
        ];

        for (const testCase of cases) {
          const runtime = makeDoraReconciliationRuntime([testCase.current], {
            ...(testCase.detail === undefined ? {} : { threadDetailSnapshot: testCase.detail }),
            ...(testCase.shell === undefined ? {} : { threadShell: testCase.shell }),
            ...(testCase.threadDetailSnapshotError === undefined
              ? {}
              : { threadDetailSnapshotError: testCase.threadDetailSnapshotError }),
            ...(testCase.threadShellError === undefined
              ? {}
              : { threadShellError: testCase.threadShellError }),
          });
          try {
            const engine = await runtime.runtime.runPromise(
              Effect.service(OrchestrationEngineService),
            );
            const dispatch = runtime.runtime.runPromise(
              engine.dispatch(
                command(`cmd-stopped-matrix-${testCase.name}`, testCase.kind, testCase.payload),
                {
                  doraActivityCapability: capability,
                },
              ),
            );
            if (testCase.accepted === true) {
              await dispatch;
              expect(runtime.appendCount()).toBe(1);
            } else {
              await expect(dispatch).rejects.toThrow();
              expect(runtime.appendCount()).toBe(0);
            }
          } finally {
            await runtime.runtime.dispose();
          }
        }
      }),
  );

  effectIt.effect("preserves the blocked-settle error and persists its rejected receipt", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const receipts = yield* OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository;
      const projectId = ProjectId.make("project-blocked-settle");
      const threadId = ThreadId.make("thread-blocked-settle");
      const commandId = CommandId.make("cmd-blocked-settle");
      const createdAt = now();

      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-blocked-settle-project-create"),
        projectId,
        title: "Project",
        workspaceRoot: "/tmp/project-blocked-settle",
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-blocked-settle-thread-create"),
        threadId,
        projectId,
        title: "Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-blocked-settle-session-set"),
        threadId,
        createdAt,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });

      const sequence = yield* engine.latestSequence;
      const error = yield* engine
        .dispatch({ type: "thread.settle", commandId, threadId })
        .pipe(Effect.flip);
      const message =
        "This thread still needs attention. Resolve or interrupt it first, then try again.";
      expect(error).toMatchObject({
        _tag: "OrchestrationThreadSettleBlockedError",
        threadId,
        message,
      });
      expect(Option.getOrNull(yield* receipts.getByCommandId({ commandId }))).toMatchObject({
        commandId,
        aggregateKind: "thread",
        aggregateId: threadId,
        status: "rejected",
        error: message,
        resultSequence: sequence,
      });
      expect(yield* engine.latestSequence).toBe(sequence);
    }).pipe(Effect.provide(makeOrchestrationLayer())),
  );

  effectIt.effect(
    "rejects persisted changes and live background work without blocking unrelated threads",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(now()));
        const engine = yield* OrchestrationEngineService;
        const snapshots = yield* ProjectionSnapshotQuery;
        const backgroundLiveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
        const projectId = ProjectId.make("project-auto-settle-guard");
        const guardedThreadId = ThreadId.make("thread-auto-settle-guarded");
        const unrelatedThreadId = ThreadId.make("thread-auto-settle-unrelated");
        const liveThreadId = ThreadId.make("thread-auto-settle-live");

        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("cmd-auto-settle-guard-project"),
          projectId,
          title: "Project",
          workspaceRoot: "/tmp/project-auto-settle-guard",
          createdAt: now(),
        });
        for (const threadId of [guardedThreadId, unrelatedThreadId, liveThreadId]) {
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`cmd-create-${threadId}`),
            threadId,
            projectId,
            title: "Thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5-codex",
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt: now(),
          });
        }

        const beforeUpdate = yield* snapshots.getSnapshot();
        const snapshotSequence = beforeUpdate.snapshotSequence;
        const originalUpdatedAt = beforeUpdate.threads.find(
          (thread) => thread.id === guardedThreadId,
        )?.updatedAt;
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-auto-settle-guard-meta"),
          threadId: guardedThreadId,
          branch: "new-branch",
        });
        const afterUpdate = yield* snapshots.getSnapshot();
        expect(afterUpdate.threads.find((thread) => thread.id === guardedThreadId)?.updatedAt).toBe(
          originalUpdatedAt,
        );

        // Automatic settlement stamps the last activity, never the sweep time.
        const lastActivityAt = "2025-12-20T00:00:00.000Z";
        const staleError = yield* engine
          .dispatch({
            type: "thread.auto-settle",
            commandId: CommandId.make("cmd-auto-settle-stale-snapshot"),
            threadId: guardedThreadId,
            snapshotSequence,
            settledAt: lastActivityAt,
          })
          .pipe(Effect.flip);
        expect(staleError._tag).toBe("OrchestrationCommandInvariantError");

        const livenessSnapshotSequence = yield* engine.latestSequence;
        for (const [taskType, expectedLiveness] of [
          ["subagent", "working"],
          ["local_bash", "monitoring"],
        ] as const) {
          backgroundLiveness.recordTaskLiveness({
            threadId: liveThreadId,
            taskId: `task-${expectedLiveness}`,
            taskType,
            status: undefined,
            kind: "started",
          });
          expect(backgroundLiveness.getThreadBackgroundLiveness(liveThreadId)).toBe(
            expectedLiveness,
          );
          expect(yield* engine.latestSequence).toBe(livenessSnapshotSequence);

          const livenessError = yield* engine
            .dispatch({
              type: "thread.auto-settle",
              commandId: CommandId.make(`cmd-auto-settle-${expectedLiveness}`),
              threadId: liveThreadId,
              snapshotSequence: livenessSnapshotSequence,
              settledAt: lastActivityAt,
            })
            .pipe(Effect.flip);
          expect(livenessError._tag).toBe("OrchestrationCommandInvariantError");
          expect(yield* engine.latestSequence).toBe(livenessSnapshotSequence);
          backgroundLiveness.clearThreadLiveness(liveThreadId);
        }

        yield* engine.dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("cmd-auto-settle-after-liveness-cleared"),
          threadId: liveThreadId,
          snapshotSequence: livenessSnapshotSequence,
          settledAt: lastActivityAt,
        });

        const freshSnapshotSequence = yield* engine.latestSequence;
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-auto-settle-unrelated-meta"),
          threadId: unrelatedThreadId,
          title: "Unrelated update",
        });
        yield* engine.dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("cmd-auto-settle-after-unrelated-update"),
          threadId: guardedThreadId,
          snapshotSequence: freshSnapshotSequence,
          settledAt: lastActivityAt,
        });

        const settled = yield* snapshots.getSnapshot();
        for (const threadId of [guardedThreadId, liveThreadId]) {
          const thread = settled.threads.find((candidate) => candidate.id === threadId);
          expect(thread?.settledOverride).toBe("settled");
          expect(thread?.settledAt).toBe(lastActivityAt);
          expect(thread?.updatedAt).toBe(now());
        }
      }).pipe(Effect.provide(makeOrchestrationLayer())),
  );

  effectIt.effect("guards client settlement against changed threads and live background work", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const snapshots = yield* ProjectionSnapshotQuery;
      const backgroundLiveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
      const projectId = ProjectId.make("project-client-settle-guard");
      const unchangedThreadId = ThreadId.make("thread-client-settle-unchanged");
      const changedThreadId = ThreadId.make("thread-client-settle-changed");
      const unrelatedThreadId = ThreadId.make("thread-client-settle-unrelated");
      const liveThreadId = ThreadId.make("thread-client-settle-live");
      const otherThreadId = ThreadId.make("thread-client-settle-other");
      const createdAt = now();

      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-client-settle-project"),
        projectId,
        title: "Project",
        workspaceRoot: "/tmp/client-settle-guard",
        createdAt,
      });
      for (const threadId of [
        unchangedThreadId,
        changedThreadId,
        unrelatedThreadId,
        liveThreadId,
        otherThreadId,
      ]) {
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`cmd-create-${threadId}`),
          threadId,
          projectId,
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt,
        });
      }

      const beforeFutureSettlement = yield* snapshots.getSnapshot();
      const futureSettlementError = yield* engine
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("cmd-client-settle-future-snapshot"),
          threadId: unchangedThreadId,
          expectedSnapshotSequence: beforeFutureSettlement.snapshotSequence + 1,
        })
        .pipe(Effect.flip);
      expect(futureSettlementError._tag).toBe("OrchestrationCommandInvariantError");
      expect(yield* engine.latestSequence).toBe(beforeFutureSettlement.snapshotSequence);
      const afterFutureSettlement = yield* snapshots.getSnapshot();
      expect(afterFutureSettlement.snapshotSequence).toBe(beforeFutureSettlement.snapshotSequence);
      expect(
        afterFutureSettlement.threads.find((thread) => thread.id === unchangedThreadId),
      ).toMatchObject({
        settledOverride: null,
        settledAt: null,
      });

      const unchangedSnapshotSequence = yield* engine.latestSequence;
      const guardedSettle = {
        type: "thread.settle" as const,
        commandId: CommandId.make("cmd-client-settle-unchanged"),
        threadId: unchangedThreadId,
        expectedSnapshotSequence: unchangedSnapshotSequence,
      };
      const firstGuardedSettle = yield* engine.dispatch(guardedSettle);
      yield* engine.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("cmd-client-settle-unchanged-unsettle"),
        threadId: unchangedThreadId,
        reason: "user",
      });
      const replayedGuardedSettle = yield* engine.dispatch(guardedSettle);
      expect(replayedGuardedSettle.sequence).toBe(firstGuardedSettle.sequence);
      const staleReplayReplacement = yield* engine
        .dispatch({
          ...guardedSettle,
          commandId: CommandId.make("cmd-client-settle-unchanged-stale-replacement"),
        })
        .pipe(Effect.flip);
      expect(staleReplayReplacement._tag).toBe("OrchestrationCommandInvariantError");

      const changedSnapshotSequence = yield* engine.latestSequence;
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-client-settle-changed-ready"),
        threadId: changedThreadId,
        createdAt,
        session: {
          threadId: changedThreadId,
          status: "ready",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });
      const changedThreadError = yield* engine
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("cmd-client-settle-changed-stale"),
          threadId: changedThreadId,
          expectedSnapshotSequence: changedSnapshotSequence,
        })
        .pipe(Effect.flip);
      expect(changedThreadError._tag).toBe("OrchestrationCommandInvariantError");
      yield* engine.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("cmd-client-settle-changed-attended"),
        threadId: changedThreadId,
      });

      const unrelatedSnapshotSequence = yield* engine.latestSequence;
      yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-client-settle-unrelated-change"),
        threadId: otherThreadId,
        title: "Changed elsewhere",
      });
      yield* engine.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("cmd-client-settle-unrelated-guarded"),
        threadId: unrelatedThreadId,
        expectedSnapshotSequence: unrelatedSnapshotSequence,
      });

      const liveSnapshotSequence = yield* engine.latestSequence;
      backgroundLiveness.recordTaskLiveness({
        threadId: liveThreadId,
        taskId: "client-settle-live-task",
        taskType: "subagent",
        status: undefined,
        kind: "started",
      });
      const liveThreadError = yield* engine
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("cmd-client-settle-live"),
          threadId: liveThreadId,
          expectedSnapshotSequence: liveSnapshotSequence,
        })
        .pipe(Effect.flip);
      expect(liveThreadError._tag).toBe("OrchestrationCommandInvariantError");
      backgroundLiveness.clearThreadLiveness(liveThreadId);
    }).pipe(Effect.provide(makeOrchestrationLayer())),
  );

  effectIt.effect("guards continuation starts against stale and nonterminal thread state", () =>
    Effect.gen(function* () {
      const createdAt = now();
      const queuedAt = "2026-01-01T00:00:01.000Z";
      yield* TestClock.setTime(Date.parse(createdAt));
      const engine = yield* OrchestrationEngineService;
      const snapshots = yield* ProjectionSnapshotQuery;
      const backgroundLiveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
      const projectId = ProjectId.make("project-turn-start-precondition");
      const providerInstanceId = ProviderInstanceId.make("codex");

      const createThread = (threadId: ThreadId) =>
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`cmd-create-${threadId}`),
          threadId,
          projectId,
          title: "Thread",
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5-codex" },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt,
        });
      const prepareReadyThread = (threadId: ThreadId) =>
        Effect.gen(function* () {
          yield* createThread(threadId);
          yield* engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`cmd-ready-${threadId}`),
            threadId,
            createdAt,
            session: {
              threadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: createdAt,
            },
          });
          yield* engine.dispatch({
            type: "thread.turn.diff.complete",
            commandId: CommandId.make(`cmd-complete-${threadId}`),
            threadId,
            turnId: TurnId.make(`turn-${threadId}`),
            completedAt: createdAt,
            checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${threadId}`),
            status: "ready",
            files: [],
            checkpointTurnCount: 1,
            createdAt,
          });
        });
      const guardedStart = (
        threadId: ThreadId,
        commandId: string,
        expectedSnapshotSequence: number,
        commandCreatedAt = createdAt,
      ) => ({
        type: "thread.turn.start" as const,
        commandId: CommandId.make(commandId),
        threadId,
        message: {
          messageId: MessageId.make(`message-${commandId}`),
          role: "user" as const,
          text: "continue",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access" as const,
        expectedSnapshotSequence,
        createdAt: commandCreatedAt,
      });
      const rejectWithoutAppend = (command: ReturnType<typeof guardedStart>) =>
        Effect.gen(function* () {
          const sequence = yield* engine.latestSequence;
          const result = yield* Effect.exit(engine.dispatch(command));
          if (Exit.isSuccess(result)) {
            const events = yield* Stream.runCollect(engine.readEvents(0));
            const matchingEvents = Array.from(events).filter(
              (event) => event.commandId === command.commandId,
            );
            const receiptKind = matchingEvents.length === 0 ? "replayed receipt" : "new append";
            throw new Error(
              `Guarded turn start '${command.commandId}' unexpectedly returned ${receiptKind} through sequence ${result.value.sequence}.`,
            );
          }
          const error = Cause.squash(result.cause);
          expect(error).toMatchObject({
            _tag: "OrchestrationCommandInvariantError",
            commandType: command.type,
          });
          expect(yield* engine.latestSequence).toBe(sequence);
        });

      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-turn-start-precondition-project"),
        projectId,
        title: "Project",
        workspaceRoot: "/tmp/turn-start-precondition",
        createdAt,
      });

      const readyThreadId = ThreadId.make("thread-turn-start-ready");
      yield* prepareReadyThread(readyThreadId);
      const readySequence = yield* engine.latestSequence;
      const accepted = guardedStart(readyThreadId, "cmd-turn-start-ready", readySequence);
      const acceptedResult = yield* engine.dispatch(accepted);
      const replayedResult = yield* engine.dispatch(accepted);
      expect(replayedResult.sequence).toBe(acceptedResult.sequence);
      const acceptedEvents = yield* Stream.runCollect(engine.readEvents(0));
      expect(
        Array.from(acceptedEvents).filter((event) => event.commandId === accepted.commandId),
      ).toHaveLength(2);
      const acceptedTurnId = TurnId.make("turn-turn-start-accepted");
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-accepted-running"),
        threadId: readyThreadId,
        createdAt,
        session: {
          threadId: readyThreadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: acceptedTurnId,
          lastError: null,
          updatedAt: createdAt,
        },
      });
      const runningDetail = yield* snapshots.getThreadDetailSnapshot(readyThreadId);
      expect(Option.getOrThrow(runningDetail).thread.latestTurn).toMatchObject({
        turnId: acceptedTurnId,
        state: "running",
        requestMessageId: accepted.message.messageId,
      });
      yield* engine.dispatch({
        type: "thread.message.assistant.complete",
        commandId: CommandId.make("cmd-turn-start-accepted-assistant-complete"),
        threadId: readyThreadId,
        messageId: MessageId.make("message-turn-start-accepted-assistant"),
        turnId: acceptedTurnId,
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-accepted-ready"),
        threadId: readyThreadId,
        createdAt,
        session: {
          threadId: readyThreadId,
          status: "ready",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });
      const completedDetail = yield* snapshots.getThreadDetailSnapshot(readyThreadId);
      expect(Option.getOrThrow(completedDetail).thread.latestTurn).toMatchObject({
        turnId: acceptedTurnId,
        state: "completed",
        requestMessageId: accepted.message.messageId,
      });

      const concurrentThreadId = ThreadId.make("thread-turn-start-concurrent");
      yield* prepareReadyThread(concurrentThreadId);
      const concurrentSequence = yield* engine.latestSequence;
      const concurrentStarts = [
        guardedStart(concurrentThreadId, "cmd-turn-start-concurrent-first", concurrentSequence),
        guardedStart(concurrentThreadId, "cmd-turn-start-concurrent-second", concurrentSequence),
      ];
      const concurrentResults = yield* Effect.all(
        concurrentStarts.map((command) => Effect.exit(engine.dispatch(command))),
        { concurrency: "unbounded" },
      );
      expect(concurrentResults.filter(Exit.isSuccess)).toHaveLength(1);
      expect(concurrentResults.filter(Exit.isFailure)).toHaveLength(1);
      const concurrentEvents = yield* Stream.runCollect(engine.readEvents(0));
      const concurrentCommandIds = new Set(concurrentStarts.map((command) => command.commandId));
      expect(
        Array.from(concurrentEvents).filter(
          (event) =>
            event.type === "thread.turn-start-requested" &&
            event.commandId !== null &&
            concurrentCommandIds.has(event.commandId),
        ),
      ).toHaveLength(1);
      const concurrentDetail = Option.getOrThrow(
        yield* snapshots.getThreadDetailSnapshot(concurrentThreadId),
      );
      expect(
        concurrentDetail.thread.messages.filter(
          (message) =>
            message.role === "user" &&
            concurrentStarts.some((command) => command.message.messageId === message.id),
        ),
      ).toHaveLength(1);

      const stoppedThreadId = ThreadId.make("thread-turn-start-stopped");
      yield* createThread(stoppedThreadId);
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-stopped-running"),
        threadId: stoppedThreadId,
        createdAt,
        session: {
          threadId: stoppedThreadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: TurnId.make("turn-turn-start-stopped"),
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-stopped-terminal"),
        threadId: stoppedThreadId,
        createdAt,
        session: {
          threadId: stoppedThreadId,
          status: "stopped",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* engine.dispatch(
        guardedStart(stoppedThreadId, "cmd-turn-start-stopped", yield* engine.latestSequence),
      );

      const failedThreadId = ThreadId.make("thread-turn-start-failed");
      yield* createThread(failedThreadId);
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-failed-running"),
        threadId: failedThreadId,
        createdAt,
        session: {
          threadId: failedThreadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: TurnId.make("turn-turn-start-failed"),
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-failed-terminal"),
        threadId: failedThreadId,
        createdAt,
        session: {
          threadId: failedThreadId,
          status: "error",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "failed",
          updatedAt: createdAt,
        },
      });
      yield* engine.dispatch(
        guardedStart(failedThreadId, "cmd-turn-start-failed", yield* engine.latestSequence),
      );

      const unrelatedThreadId = ThreadId.make("thread-turn-start-unrelated");
      const unrelatedChangeThreadId = ThreadId.make("thread-turn-start-unrelated-change");
      yield* prepareReadyThread(unrelatedThreadId);
      yield* createThread(unrelatedChangeThreadId);
      const unrelatedSequence = yield* engine.latestSequence;
      yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-turn-start-unrelated-change"),
        threadId: unrelatedChangeThreadId,
        title: "Changed elsewhere",
      });
      yield* engine.dispatch(
        guardedStart(unrelatedThreadId, "cmd-turn-start-unrelated", unrelatedSequence),
      );

      const staleThreadId = ThreadId.make("thread-turn-start-stale");
      yield* prepareReadyThread(staleThreadId);
      const staleSequence = yield* engine.latestSequence;
      yield* engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stale-work"),
        threadId: staleThreadId,
        message: {
          messageId: MessageId.make("message-turn-start-stale-work"),
          role: "user",
          text: "new work",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt,
      });
      yield* rejectWithoutAppend(
        guardedStart(staleThreadId, "cmd-turn-start-stale", staleSequence),
      );

      const futureThreadId = ThreadId.make("thread-turn-start-future");
      yield* prepareReadyThread(futureThreadId);
      const futureSequence = yield* engine.latestSequence;
      yield* rejectWithoutAppend(
        guardedStart(futureThreadId, "cmd-turn-start-future", futureSequence + 1),
      );
      yield* rejectWithoutAppend(
        guardedStart(
          ThreadId.make("thread-turn-start-missing"),
          "cmd-turn-start-missing",
          futureSequence,
        ),
      );

      const bootstrapThreadId = ThreadId.make("thread-turn-start-bootstrap");
      yield* prepareReadyThread(bootstrapThreadId);
      const bootstrapSequence = yield* engine.latestSequence;
      const bootstrapCommand = {
        ...guardedStart(bootstrapThreadId, "cmd-turn-start-bootstrap", bootstrapSequence),
        bootstrap: { runSetupScript: true },
      };
      yield* rejectWithoutAppend(bootstrapCommand);

      const startingThreadId = ThreadId.make("thread-turn-start-starting");
      yield* prepareReadyThread(startingThreadId);
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-starting-session"),
        threadId: startingThreadId,
        createdAt,
        session: {
          threadId: startingThreadId,
          status: "starting",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* rejectWithoutAppend(
        guardedStart(startingThreadId, "cmd-turn-start-starting", yield* engine.latestSequence),
      );

      const runningThreadId = ThreadId.make("thread-turn-start-running");
      yield* prepareReadyThread(runningThreadId);
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-running-session"),
        threadId: runningThreadId,
        createdAt,
        session: {
          threadId: runningThreadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: TurnId.make("turn-turn-start-running"),
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* rejectWithoutAppend(
        guardedStart(runningThreadId, "cmd-turn-start-running", yield* engine.latestSequence),
      );

      const activeTurnThreadId = ThreadId.make("thread-turn-start-active-turn");
      yield* prepareReadyThread(activeTurnThreadId);
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-turn-start-active-turn-session"),
        threadId: activeTurnThreadId,
        createdAt,
        session: {
          threadId: activeTurnThreadId,
          status: "ready",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: TurnId.make("turn-turn-start-active"),
          lastError: null,
          updatedAt: createdAt,
        },
      });
      yield* rejectWithoutAppend(
        guardedStart(
          activeTurnThreadId,
          "cmd-turn-start-active-turn",
          yield* engine.latestSequence,
        ),
      );

      const pendingThreadId = ThreadId.make("thread-turn-start-pending");
      yield* prepareReadyThread(pendingThreadId);
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-turn-start-pending-approval"),
        threadId: pendingThreadId,
        createdAt,
        activity: {
          id: EventId.make("activity-turn-start-pending-approval"),
          kind: "approval.requested",
          summary: "Approval requested",
          tone: "approval",
          turnId: null,
          createdAt,
          payload: {
            requestId: "approval-turn-start-pending",
            requestKind: "command",
            detail: "Approve the pending command.",
          },
        },
      });
      expect(
        Option.getOrThrow(yield* snapshots.getThreadShellById(pendingThreadId)).hasPendingApprovals,
      ).toBe(true);
      yield* rejectWithoutAppend(
        guardedStart(pendingThreadId, "cmd-turn-start-pending", yield* engine.latestSequence),
      );

      const pendingInputThreadId = ThreadId.make("thread-turn-start-pending-input");
      yield* prepareReadyThread(pendingInputThreadId);
      const pendingInputAppendReceipt = yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-turn-start-pending-input-append"),
        threadId: pendingInputThreadId,
        createdAt,
        activity: {
          id: EventId.make("activity-turn-start-pending-input"),
          kind: "user-input.requested",
          summary: "Input requested",
          tone: "info",
          turnId: null,
          createdAt,
          payload: {
            requestId: "input-turn-start-pending",
            responseMode: "message",
            questions: [
              {
                id: "continue",
                header: "Continue",
                question: "Continue the current work?",
                options: [{ label: "yes", description: "Continue" }],
              },
            ],
          },
        },
      });
      const pendingInputDetail = yield* snapshots.getThreadDetailSnapshot(pendingInputThreadId);
      expect(pendingInputAppendReceipt.sequence).toBe(yield* engine.latestSequence);
      expect(
        Option.getOrThrow(pendingInputDetail).thread.activities.some(
          (activity) => activity.id === EventId.make("activity-turn-start-pending-input"),
        ),
      ).toBe(true);
      expect(
        Option.getOrThrow(yield* snapshots.getThreadShellById(pendingInputThreadId))
          .hasPendingUserInput,
      ).toBe(true);
      yield* rejectWithoutAppend(
        guardedStart(
          pendingInputThreadId,
          "cmd-turn-start-pending-input",
          yield* engine.latestSequence,
        ),
      );

      for (const queuedCase of [
        {
          name: "ready-at-clock-equality",
          status: "ready" as const,
          queuedAt,
          clockAt: queuedAt,
        },
        {
          name: "error-at-clock-equality",
          status: "error" as const,
          queuedAt,
          clockAt: queuedAt,
        },
        {
          name: "stopped-with-old-message",
          status: "stopped" as const,
          queuedAt: "2025-12-31T23:57:00.000Z",
          clockAt: createdAt,
        },
      ]) {
        const threadId = ThreadId.make(`thread-turn-start-queued-${queuedCase.name}`);
        yield* prepareReadyThread(threadId);
        if (queuedCase.status !== "ready") {
          yield* engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`cmd-turn-start-queued-${queuedCase.name}-session`),
            threadId,
            createdAt,
            session: {
              threadId,
              status: queuedCase.status,
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: queuedCase.status === "error" ? "failed" : null,
              updatedAt: createdAt,
            },
          });
        }
        yield* TestClock.setTime(Date.parse(queuedCase.clockAt));
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`cmd-turn-start-queued-${queuedCase.name}-work`),
          threadId,
          message: {
            messageId: MessageId.make(`message-turn-start-queued-${queuedCase.name}-work`),
            role: "user",
            text: "queued work",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          createdAt: queuedCase.queuedAt,
        });
        yield* rejectWithoutAppend(
          guardedStart(
            threadId,
            `cmd-turn-start-queued-${queuedCase.name}`,
            yield* engine.latestSequence,
            queuedCase.queuedAt,
          ),
        );
      }

      const liveThreadId = ThreadId.make("thread-turn-start-live");
      yield* prepareReadyThread(liveThreadId);
      backgroundLiveness.recordTaskLiveness({
        threadId: liveThreadId,
        taskId: "task-turn-start-live",
        taskType: "subagent",
        status: undefined,
        kind: "started",
      });
      yield* rejectWithoutAppend(
        guardedStart(liveThreadId, "cmd-turn-start-live", yield* engine.latestSequence),
      );
      backgroundLiveness.clearThreadLiveness(liveThreadId);
    }).pipe(Effect.provide(makeOrchestrationLayer())),
  );

  it("persists deterministic read models for repeated snapshot reads", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-1-create"),
        projectId: asProjectId("project-1"),
        title: "Project 1",
        workspaceRoot: "/tmp/project-1",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-1-create"),
        threadId: ThreadId.make("thread-1"),
        projectId: asProjectId("project-1"),
        title: "Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("msg-1"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt,
      }),
    );

    const readModelA = await system.readModel();
    const readModelB = await system.readModel();
    expect(readModelB).toEqual(readModelA);
    await system.dispose();
  });

  it("archives and unarchives threads through orchestration commands", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-archive-create"),
        projectId: asProjectId("project-archive"),
        title: "Project Archive",
        workspaceRoot: "/tmp/project-archive",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-archive-create"),
        threadId: ThreadId.make("thread-archive"),
        projectId: asProjectId("project-archive"),
        title: "Archive me",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-archive-title-regeneration"),
        threadId: ThreadId.make("thread-archive"),
        regenerateTitle: true,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("cmd-thread-archive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).not.toBeNull();
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.titleRegeneration,
    ).toBeNull();

    await system.run(
      engine.dispatch({
        type: "thread.unarchive",
        commandId: CommandId.make("cmd-thread-unarchive"),
        threadId: ThreadId.make("thread-archive"),
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.archivedAt,
    ).toBeNull();
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")
        ?.titleRegeneration,
    ).toBeNull();
    await system.run(
      engine.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("cmd-thread-archive-stale-title-completion"),
        threadId: ThreadId.make("thread-archive"),
        requestId: CommandId.make("cmd-thread-archive-title-regeneration"),
        title: "Stale generated title",
      }),
    );
    expect(
      (await system.readModel()).threads.find((thread) => thread.id === "thread-archive")?.title,
    ).toBe("Archive me");

    await system.dispose();
  });

  it("replays append-only events from sequence", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-replay-create"),
        projectId: asProjectId("project-replay"),
        title: "Replay Project",
        workspaceRoot: "/tmp/project-replay",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-replay-create"),
        threadId: ThreadId.make("thread-replay"),
        projectId: asProjectId("project-replay"),
        title: "replay",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("cmd-thread-replay-delete"),
        threadId: ThreadId.make("thread-replay"),
      }),
    );

    const events = await system.run(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(events.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.deleted",
    ]);
    await system.dispose();
  });

  it("streams persisted domain events in order", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-stream-create"),
        projectId: asProjectId("project-stream"),
        title: "Stream Project",
        workspaceRoot: "/tmp/project-stream",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    const eventTypes: string[] = [];
    await system.run(
      Effect.gen(function* () {
        const eventQueue = yield* Queue.unbounded<OrchestrationEvent>();
        yield* Effect.forkScoped(
          Stream.take(engine.streamDomainEvents, 2).pipe(
            Stream.runForEach((event) => Queue.offer(eventQueue, event).pipe(Effect.asVoid)),
          ),
        );
        yield* Effect.sleep("10 millis");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-stream-thread-create"),
          threadId: ThreadId.make("thread-stream"),
          projectId: asProjectId("project-stream"),
          title: "domain-stream",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        });
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-stream-thread-update"),
          threadId: ThreadId.make("thread-stream"),
          title: "domain-stream-updated",
        });
        eventTypes.push((yield* Queue.take(eventQueue)).type);
        eventTypes.push((yield* Queue.take(eventQueue)).type);
      }).pipe(Effect.scoped),
    );

    expect(eventTypes).toEqual(["thread.created", "thread.meta-updated"]);
    await system.dispose();
  });

  it("does not regress a generated branch to a stale temporary worktree branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-branch-race-project-create"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Project",
        workspaceRoot: "/tmp/project-branch-race",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-branch-race-thread-create"),
        threadId: ThreadId.make("thread-branch-race"),
        projectId: asProjectId("project-branch-race"),
        title: "Branch Race Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "t3code/generated-branch-name",
        worktreePath: "/tmp/project-branch-race-worktree",
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-stale-temporary-branch-sync"),
        threadId: ThreadId.make("thread-branch-race"),
        branch: "t3code/1234abcd",
        expectedBranch: "t3code/1234abcd",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/generated-branch-name");
    await system.dispose();
  });

  it("allows authoritative worktree bootstrap to assign a temporary branch", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-project-create"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Project",
        workspaceRoot: "/tmp/project-worktree-bootstrap",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-worktree-bootstrap-thread-create"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        projectId: asProjectId("project-worktree-bootstrap"),
        title: "Worktree Bootstrap Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: "main",
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-authoritative-worktree-bootstrap"),
        threadId: ThreadId.make("thread-worktree-bootstrap"),
        branch: "t3code/1234abcd",
        worktreePath: "/tmp/project-worktree-bootstrap-worktree",
      }),
    );

    const snapshot = await system.readModel();
    expect(snapshot.threads[0]?.branch).toBe("t3code/1234abcd");
    expect(snapshot.threads[0]?.worktreePath).toBe("/tmp/project-worktree-bootstrap-worktree");
    await system.dispose();
  });

  it("records command ack duration using the first committed event type", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-ack-create"),
        projectId: asProjectId("project-ack"),
        title: "Ack Project",
        workspaceRoot: "/tmp/project-ack",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-ack-create"),
        threadId: ThreadId.make("thread-ack"),
        projectId: asProjectId("project-ack"),
        title: "Ack Thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_command_ack_duration", {
        commandType: "thread.create",
        aggregateKind: "thread",
        ackEventType: "thread.created",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("records failed command dispatches as metric failures", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-missing-project"),
          threadId: ThreadId.make("thread-missing-project"),
          projectId: asProjectId("project-missing"),
          title: "Missing Project Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("does not exist");

    const snapshots = await system.run(Metric.snapshot);
    expect(
      hasMetricSnapshot(snapshots, "t3_orchestration_commands_total", {
        commandType: "thread.create",
        aggregateKind: "thread",
        outcome: "failure",
      }),
    ).toBe(true);

    await system.dispose();
  });

  it("stores completed checkpoint summaries even when no files changed", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-turn-diff-create"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn Diff Project",
        workspaceRoot: "/tmp/project-turn-diff",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-turn-diff-create"),
        threadId: ThreadId.make("thread-turn-diff"),
        projectId: asProjectId("project-turn-diff"),
        title: "Turn diff thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-turn-diff-complete"),
        threadId: ThreadId.make("thread-turn-diff"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );

    const thread = (await system.readModel()).threads.find(
      (entry) => entry.id === "thread-turn-diff",
    );
    expect(thread?.checkpoints).toEqual([
      {
        turnId: asTurnId("turn-1"),
        checkpointTurnCount: 1,
        checkpointRef: asCheckpointRef("refs/t3/checkpoints/thread-turn-diff/turn/1"),
        status: "ready",
        files: [],
        assistantMessageId: null,
        completedAt: createdAt,
      },
    ]);
    await system.dispose();
  });

  it("keeps processing queued commands after a storage failure", async () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;
    let shouldFailFirstAppend = true;

    const flakyStore: OrchestrationEventStoreShape = {
      append(event) {
        if (shouldFailFirstAppend && event.commandId === CommandId.make("cmd-flaky-1")) {
          shouldFailFirstAppend = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.append",
              detail: "append failed",
            }),
          );
        }
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
      hasEventAfter: () => Effect.succeed(false),
    };

    const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-orchestration-engine-test-",
    });

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(ThreadBackgroundLiveness.layer),
        Layer.provide(ThreadPlanProgress.layer),
        Layer.provide(OrchestrationProjectionPipelineLive),
        Layer.provide(Layer.succeed(OrchestrationEventStore, flakyStore)),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provideMerge(ServerConfigLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-flaky-create"),
        projectId: asProjectId("project-flaky"),
        title: "Flaky Project",
        workspaceRoot: "/tmp/project-flaky",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await expect(
      runtime.runPromise(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-flaky-1"),
          threadId: ThreadId.make("thread-flaky-fail"),
          projectId: asProjectId("project-flaky"),
          title: "flaky-fail",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("append failed");

    const result = await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-flaky-2"),
        threadId: ThreadId.make("thread-flaky-ok"),
        projectId: asProjectId("project-flaky"),
        title: "flaky-ok",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    expect(result.sequence).toBe(2);
    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);
    await runtime.dispose();
  });

  it("rolls back all events for a multi-event command when projection fails mid-dispatch", async () => {
    let shouldFailRequestedProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: () => Effect.void,
      projectEventDeferred: (event) => {
        if (
          shouldFailRequestedProjection &&
          event.commandId === CommandId.make("cmd-turn-start-atomic") &&
          event.type === "thread.turn-start-requested"
        ) {
          shouldFailRequestedProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.succeed(Effect.void);
      },
    };

    const runtime = ManagedRuntime.make(
      OrchestrationEngineLive.pipe(
        Layer.provide(OrchestrationProjectionSnapshotQueryLive),
        Layer.provide(ThreadBackgroundLiveness.layer),
        Layer.provide(ThreadPlanProgress.layer),
        Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
        Layer.provide(OrchestrationEventStoreLive),
        Layer.provide(OrchestrationCommandReceiptRepositoryLive),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(SqlitePersistenceMemory),
        Layer.provide(NodeServices.layer),
      ),
    );
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const createdAt = now();

    await runtime.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-atomic-create"),
        projectId: asProjectId("project-atomic"),
        title: "Atomic Project",
        workspaceRoot: "/tmp/project-atomic",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await runtime.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-atomic-create"),
        threadId: ThreadId.make("thread-atomic"),
        projectId: asProjectId("project-atomic"),
        title: "atomic",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const turnStartCommand = {
      type: "thread.turn.start" as const,
      commandId: CommandId.make("cmd-turn-start-atomic"),
      threadId: ThreadId.make("thread-atomic"),
      message: {
        messageId: asMessageId("msg-atomic-1"),
        role: "user" as const,
        text: "hello",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required" as const,
      createdAt,
    };

    await expect(runtime.runPromise(engine.dispatch(turnStartCommand))).rejects.toThrow(
      "projection failed",
    );

    const eventsAfterFailure = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterFailure.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
    ]);

    const retryResult = await runtime.runPromise(engine.dispatch(turnStartCommand));
    expect(retryResult.sequence).toBe(4);

    const eventsAfterRetry = await runtime.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(
        Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)),
      ),
    );
    expect(eventsAfterRetry.map((event) => event.type)).toEqual([
      "project.created",
      "thread.created",
      "thread.message-sent",
      "thread.turn-start-requested",
    ]);
    expect(
      eventsAfterRetry.filter((event) => event.commandId === turnStartCommand.commandId),
    ).toHaveLength(2);

    await runtime.dispose();
  });

  effectIt.effect("reconciles command state when append persists but projection fails", () => {
    type StoredEvent =
      ReturnType<OrchestrationEventStoreShape["append"]> extends Effect.Effect<infer A, any, any>
        ? A
        : never;
    const events: StoredEvent[] = [];
    let nextSequence = 1;

    const nonTransactionalStore: OrchestrationEventStoreShape = {
      append(event) {
        const savedEvent = {
          ...event,
          sequence: nextSequence,
        } as StoredEvent;
        nextSequence += 1;
        events.push(savedEvent);
        return Effect.succeed(savedEvent);
      },
      readFromSequence(sequenceExclusive) {
        return Stream.fromIterable(events.filter((event) => event.sequence > sequenceExclusive));
      },
      readAll() {
        return Stream.fromIterable(events);
      },
      hasEventAfter: () => Effect.succeed(false),
    };

    let shouldFailProjection = true;
    const flakyProjectionPipeline: OrchestrationProjectionPipelineShape = {
      bootstrap: Effect.void,
      projectEvent: () => Effect.void,
      projectEventDeferred: (event) => {
        if (
          shouldFailProjection &&
          event.commandId === CommandId.make("cmd-thread-archive-sync-fail")
        ) {
          shouldFailProjection = false;
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.projection",
              detail: "projection failed",
            }),
          );
        }
        return Effect.succeed(Effect.void);
      },
    };

    return Effect.gen(function* () {
      const engine = yield* Effect.service(OrchestrationEngineService);
      const createdAt = now();

      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-sync-create"),
        projectId: asProjectId("project-sync"),
        title: "Sync Project",
        workspaceRoot: "/tmp/project-sync",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-sync-create"),
        threadId: ThreadId.make("thread-sync"),
        projectId: asProjectId("project-sync"),
        title: "sync-before",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      });

      const projectionError = yield* engine
        .dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-fail"),
          threadId: ThreadId.make("thread-sync"),
        })
        .pipe(Effect.flip);
      expect(projectionError.message).toContain("projection failed");

      const retryError = yield* engine
        .dispatch({
          type: "thread.archive",
          commandId: CommandId.make("cmd-thread-archive-sync-retry"),
          threadId: ThreadId.make("thread-sync"),
        })
        .pipe(Effect.flip);
      expect(retryError.message).toContain("already archived");
    }).pipe(
      Effect.provide(
        OrchestrationEngineLive.pipe(
          Layer.provide(OrchestrationProjectionSnapshotQueryLive),
          Layer.provide(ThreadBackgroundLiveness.layer),
          Layer.provide(ThreadPlanProgress.layer),
          Layer.provide(Layer.succeed(OrchestrationProjectionPipeline, flakyProjectionPipeline)),
          Layer.provide(Layer.succeed(OrchestrationEventStore, nonTransactionalStore)),
          Layer.provide(OrchestrationCommandReceiptRepositoryLive),
          Layer.provide(RepositoryIdentityResolver.layer),
          Layer.provide(SqlitePersistenceMemory),
          Layer.provide(NodeServices.layer),
        ),
      ),
    );
  });

  it("fails command dispatch when command invariants are violated", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-invariant-missing-thread"),
          threadId: ThreadId.make("thread-missing"),
          message: {
            messageId: asMessageId("msg-missing"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now(),
        }),
      ),
    ).rejects.toThrow("Thread 'thread-missing' does not exist");

    await system.dispose();
  });

  it("rejects duplicate thread creation", async () => {
    const system = await createOrchestrationSystem();
    const { engine } = system;
    const createdAt = now();

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-duplicate-create"),
        projectId: asProjectId("project-duplicate"),
        title: "Duplicate Project",
        workspaceRoot: "/tmp/project-duplicate",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-duplicate-1"),
        threadId: ThreadId.make("thread-duplicate"),
        projectId: asProjectId("project-duplicate"),
        title: "duplicate",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-duplicate-2"),
          threadId: ThreadId.make("thread-duplicate"),
          projectId: asProjectId("project-duplicate"),
          title: "duplicate",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      ),
    ).rejects.toThrow("already exists");

    await system.dispose();
  });

  it("replays the accepted receipt for a genuine retry of the same command", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-retry-project-create"),
        projectId: asProjectId("project-retry"),
        title: "Retry Project",
        workspaceRoot: "/tmp/project-retry",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await system.run(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-retry-thread-create"),
        threadId: ThreadId.make("thread-retry"),
        projectId: asProjectId("project-retry"),
        title: "retry",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt,
      }),
    );

    const turnStart = {
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-retry-turn-start"),
      threadId: ThreadId.make("thread-retry"),
      message: {
        messageId: asMessageId("msg-retry"),
        role: "user",
        text: "hello",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      createdAt,
    } as const;

    const first = await system.run(engine.dispatch(turnStart));
    const second = await system.run(engine.dispatch(turnStart));
    expect(second.sequence).toBe(first.sequence);

    const readModel = await system.readModel();
    const thread = readModel.threads.find((candidate) => candidate.id === "thread-retry");
    expect(thread?.messages.filter((message) => message.role === "user")).toHaveLength(1);

    await system.dispose();
  });

  it("rejects reusing an accepted command id for a different aggregate", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-conflict-project-create"),
        projectId: asProjectId("project-conflict"),
        title: "Conflict Project",
        workspaceRoot: "/tmp/project-conflict",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    for (const threadId of ["thread-conflict-a", "thread-conflict-b"]) {
      await system.run(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`cmd-${threadId}-create`),
          threadId: ThreadId.make(threadId),
          projectId: asProjectId("project-conflict"),
          title: threadId,
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        }),
      );
    }

    await system.run(
      engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-conflict-turn-start"),
        threadId: ThreadId.make("thread-conflict-a"),
        message: {
          messageId: asMessageId("msg-conflict-a"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt,
      }),
    );

    await expect(
      system.run(
        engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-conflict-turn-start"),
          threadId: ThreadId.make("thread-conflict-b"),
          message: {
            messageId: asMessageId("msg-conflict-b"),
            role: "user",
            text: "hello again",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        }),
      ),
    ).rejects.toThrow("already used for thread 'thread-conflict-a'");

    const readModel = await system.readModel();
    const targetThread = readModel.threads.find(
      (candidate) => candidate.id === "thread-conflict-b",
    );
    expect(targetThread?.messages.filter((message) => message.role === "user")).toHaveLength(0);

    await system.dispose();
  });

  it("stamps the dispatching client's origin onto persisted event metadata", async () => {
    const createdAt = now();
    const system = await createOrchestrationSystem();
    const { engine } = system;

    await system.run(
      engine.dispatch(
        {
          type: "project.create",
          commandId: CommandId.make("cmd-origin-project-create"),
          projectId: asProjectId("project-origin"),
          title: "Origin Project",
          workspaceRoot: "/tmp/project-origin",
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          createdAt,
        },
        { origin: { surface: "mobile", appVersion: "1.2.3" } },
      ),
    );
    await system.run(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-no-origin-project-create"),
        projectId: asProjectId("project-no-origin"),
        title: "No Origin Project",
        workspaceRoot: "/tmp/project-no-origin",
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );

    const events = await system.run(
      Stream.runCollect(engine.readEvents(0)).pipe(Effect.map((chunk) => Array.from(chunk))),
    );
    const withOrigin = events.find((event) => event.commandId === "cmd-origin-project-create");
    const withoutOrigin = events.find(
      (event) => event.commandId === "cmd-no-origin-project-create",
    );

    expect(withOrigin?.metadata.origin).toEqual({ surface: "mobile", appVersion: "1.2.3" });
    expect(withoutOrigin?.metadata.origin).toBeUndefined();

    await system.dispose();
  });

  effectIt.effect(
    "accepts Dora activities only through a session-bound trusted provider capability",
    () =>
      Effect.promise(async () => {
        const system = await createOrchestrationSystem();
        const { engine } = system;
        const projectId = ProjectId.make("project-dora-boundary");
        const threadId = ThreadId.make("thread-dora-boundary");
        const createdAt = "2026-01-01T00:00:00.000Z";
        const providerInstanceId = ProviderInstanceId.make("dora");
        const providerSessionId = ProviderSessionId.make("dora-session-1");
        const activity = (
          kind: "dora.plan" | "dora.replan" | "dora.verification" | "dora.side-effect",
          commandId: CommandId,
        ) => ({
          type: "thread.activity.append" as const,
          commandId,
          threadId,
          providerInstanceId,
          providerSessionId,
          activity: {
            id: EventId.make(`activity-${kind}`),
            tone: "info" as const,
            kind,
            summary: `${kind} completed`,
            payload: { steps: ["inspect", "verify"] },
            turnId: null,
            createdAt: "2031-01-01T00:00:00.000Z",
          },
          createdAt: "2031-01-01T00:00:00.000Z",
        });
        try {
          await system.run(
            engine.dispatch({
              type: "project.create",
              commandId: CommandId.make("cmd-dora-project"),
              projectId,
              title: "Dora project",
              workspaceRoot: "/tmp/dora-boundary",
              createdAt,
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.create",
              commandId: CommandId.make("cmd-dora-thread"),
              threadId,
              projectId,
              title: "Dora thread",
              modelSelection: { instanceId: providerInstanceId, model: "dora" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt,
            }),
          );

          await expect(
            system.run(
              engine.dispatch(
                activity("dora.plan", CommandId.make("cmd-dora-plan-unauthenticated")),
              ),
            ),
          ).rejects.toThrow("authenticated control-plane capability");

          await system.run(
            engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-dora-session"),
              threadId,
              createdAt,
              session: {
                threadId,
                status: "ready",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "full-access",
                activeTurnId: null,
                lastError: null,
                updatedAt: createdAt,
              },
            }),
          );
          const capability = createAuthenticatedDoraActivityCapability({
            threadId,
            providerInstanceId,
            providerSessionId,
          });
          await expect(
            system.run(
              engine.dispatch(
                activity("dora.plan", CommandId.make("cmd-dora-plan-wrong-session")),
                {
                  doraActivityCapability: createAuthenticatedDoraActivityCapability({
                    threadId,
                    providerInstanceId,
                    providerSessionId: ProviderSessionId.make("wrong-session"),
                  }),
                },
              ),
            ),
          ).rejects.toThrow(
            "Dora activity does not carry the authenticated provider session binding.",
          );
          for (const kind of [
            "dora.plan",
            "dora.replan",
            "dora.verification",
            "dora.side-effect",
          ] as const) {
            await system.run(
              engine.dispatch(activity(kind, CommandId.make(`cmd-dora-${kind}-accepted`)), {
                doraActivityCapability: capability,
              }),
            );
          }
          const events = await system.run(
            Stream.runCollect(engine.readEvents(0)).pipe(Effect.map((chunk) => Array.from(chunk))),
          );
          const doraActivities = events.filter(
            (event) =>
              event.type === "thread.activity-appended" &&
              event.payload.activity.kind.startsWith("dora."),
          );
          expect(doraActivities).toHaveLength(4);
          for (const event of doraActivities) {
            if (event.type !== "thread.activity-appended") continue;
            expect(event.payload.activity.createdAt).toBe(event.occurredAt);
            expect(event.payload.activity.createdAt).not.toBe("2031-01-01T00:00:00.000Z");
          }

          const secretBearingActivity = activity("dora.plan", CommandId.make("cmd-dora-secret"));
          await expect(
            system.run(
              engine.dispatch(
                {
                  ...secretBearingActivity,
                  activity: {
                    ...secretBearingActivity.activity,
                    summary: "authorization: Bearer private-secret",
                  },
                },
                { doraActivityCapability: capability },
              ),
            ),
          ).rejects.toThrow("secret-bearing");

          const excessivelyDeepActivity = activity("dora.plan", CommandId.make("cmd-dora-depth"));
          await expect(
            system.run(
              engine.dispatch(
                {
                  ...excessivelyDeepActivity,
                  activity: {
                    ...excessivelyDeepActivity.activity,
                    payload: {
                      one: {
                        two: {
                          three: {
                            four: {
                              five: { six: "deep" },
                            },
                          },
                        },
                      },
                    },
                  },
                },
                { doraActivityCapability: capability },
              ),
            ),
          ).rejects.toThrow("maximum depth");
        } finally {
          await system.dispose();
        }
      }),
  );

  effectIt.effect(
    "allows a clean stopped completed Dora session to finish closure activity and settle",
    () =>
      Effect.promise(async () => {
        const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-dora-closure-"));
        const databasePath = NodePath.join(directory, "state.sqlite");
        let system = await createOrchestrationSystem(databasePath);
        const { engine } = system;
        const projectId = ProjectId.make("dora-backlog-ralph-trades-v3-20260907");
        const threadId = ThreadId.make("dora-thread-jdrolls/ralph-trades-v3-1678");
        const providerInstanceId = ProviderInstanceId.make("dora_canary");
        const providerSessionId = ProviderSessionId.make("ses_f83eff670ffeHGH74jVw3Pfk6i");
        const turnId = TurnId.make("dora-turn-1788862407846-76");
        const assistantMessageId = MessageId.make("assistant:dora-turn-1788862407846-76");
        const completedAt = "2026-09-08T10:16:12.719Z";
        const capability = createAuthenticatedDoraActivityCapability({
          threadId,
          providerInstanceId,
          providerSessionId,
        });
        const appendActivity = (
          commandId: string,
          kind: "dora.verification" | "dora.side-effect",
          payload: Record<string, unknown>,
        ) =>
          system.engine.dispatch(
            {
              type: "thread.activity.append",
              commandId: CommandId.make(commandId),
              threadId,
              providerInstanceId,
              providerSessionId,
              activity: {
                id: EventId.make(`activity-${commandId}`),
                tone: "info",
                kind,
                summary: `${kind} completed`,
                payload,
                turnId: null,
                createdAt: now(),
              },
              createdAt: now(),
            },
            { doraActivityCapability: capability },
          );
        try {
          await system.run(
            engine.dispatch({
              type: "project.create",
              commandId: CommandId.make("cmd-stopped-dora-project"),
              projectId,
              title: "Dora project",
              workspaceRoot: "/tmp/dora-stopped-original1678",
              createdAt: now(),
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.create",
              commandId: CommandId.make("cmd-stopped-dora-thread"),
              threadId,
              projectId,
              title: "Dora original 1678",
              modelSelection: { instanceId: providerInstanceId, model: "default" },
              runtimeMode: "auto",
              interactionMode: "default",
              branch: "main",
              worktreePath: "/tmp/dora-stopped-original1678",
              createdAt: now(),
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.turn.start",
              commandId: CommandId.make("cmd-stopped-dora-turn"),
              threadId,
              message: {
                messageId: MessageId.make(
                  "dora-message-retry-3-6c1552aaa92d81e192eeae0f1980e42a-jdrolls/ralph-trades-v3-1678",
                ),
                role: "user",
                text: "Verify original 1678",
                attachments: [],
              },
              interactionMode: "default",
              runtimeMode: "auto",
              createdAt: completedAt,
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-stopped-dora-running"),
              threadId,
              createdAt: completedAt,
              session: {
                threadId,
                status: "running",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "auto",
                activeTurnId: turnId,
                lastError: null,
                updatedAt: completedAt,
              },
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.message.assistant.delta",
              commandId: CommandId.make("cmd-stopped-dora-assistant-delta"),
              threadId,
              messageId: assistantMessageId,
              turnId,
              delta: '{"verdict":"pass"}',
              createdAt: completedAt,
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.message.assistant.complete",
              commandId: CommandId.make("cmd-stopped-dora-assistant-complete"),
              threadId,
              messageId: assistantMessageId,
              turnId,
              createdAt: completedAt,
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-stopped-dora-ready"),
              threadId,
              createdAt: completedAt,
              session: {
                threadId,
                status: "ready",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "auto",
                activeTurnId: null,
                lastError: null,
                updatedAt: completedAt,
              },
            }),
          );
          await system.run(
            engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-stopped-dora-terminal"),
              threadId,
              createdAt: "2026-09-08T10:46:30.434Z",
              session: {
                threadId,
                status: "stopped",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "auto",
                activeTurnId: null,
                lastError: null,
                updatedAt: "2026-09-08T10:46:30.434Z",
              },
            }),
          );
          const beforeClosure = await system.readThread(threadId);
          expect(Option.getOrThrow(beforeClosure)).toMatchObject({
            latestTurn: {
              turnId,
              state: "completed",
              completedAt,
              assistantMessageId,
            },
            session: {
              status: "stopped",
              providerName: "dora",
              providerInstanceId,
              providerSessionId,
              activeTurnId: null,
              lastError: null,
            },
          });

          await system.dispose();
          system = await createOrchestrationSystem(databasePath);
          const coldCommandThread = (await system.commandReadModel()).threads.find(
            (thread) => thread.id === threadId,
          );
          expect(coldCommandThread).toMatchObject({
            latestTurn: { turnId, state: "completed", completedAt, assistantMessageId },
            session: {
              status: "stopped",
              providerName: "dora",
              providerInstanceId,
              providerSessionId,
              activeTurnId: null,
              lastError: null,
            },
            messages: [],
            activities: [],
          });

          await expect(
            system.run(
              appendActivity("cmd-stopped-dora-time-gate", "dora.side-effect", {
                kind: "time-gate-comment",
              }),
            ),
          ).rejects.toThrow("active bound Dora session");
          const sql = await system.sql();
          await system.runSql(sql`
            INSERT INTO projection_turns (
              thread_id,
              turn_id,
              pending_message_id,
              source_proposed_plan_thread_id,
              source_proposed_plan_id,
              assistant_message_id,
              state,
              requested_at,
              started_at,
              completed_at,
              checkpoint_turn_count,
              checkpoint_ref,
              checkpoint_status,
              checkpoint_files_json
            )
            VALUES (
              ${threadId},
              NULL,
              ${MessageId.make("queued-stopped-dora-message")},
              NULL,
              NULL,
              NULL,
              'pending',
              ${now()},
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              '[]'
            )
          `);
          expect(Option.getOrThrow(await system.readThread(threadId)).latestTurn).toMatchObject({
            state: "completed",
            completedAt,
          });
          await expect(
            system.run(appendActivity("cmd-stopped-dora-queued", "dora.verification", {})),
          ).rejects.toThrow("clean completed stopped Dora session");
          await system.runSql(sql`
            DELETE FROM projection_turns
            WHERE thread_id = ${threadId}
              AND turn_id IS NULL
              AND state = 'pending'
          `);
          const backgroundLiveness = await system.backgroundLiveness();
          backgroundLiveness.recordTaskLiveness({
            threadId,
            taskId: "stopped-dora-live-task",
            taskType: "subagent",
            status: undefined,
            kind: "started",
          });
          await expect(
            system.run(appendActivity("cmd-stopped-dora-live", "dora.verification", {})),
          ).rejects.toThrow("clean completed stopped Dora session");
          backgroundLiveness.clearThreadLiveness(threadId);
          await system.run(
            appendActivity("cmd-stopped-dora-verification", "dora.verification", {}),
          );
          await system.run(
            appendActivity("cmd-stopped-dora-deployment", "dora.side-effect", {
              kind: "deployment",
            }),
          );
          const closeIssue = await system.run(
            appendActivity("cmd-stopped-dora-close-issue", "dora.side-effect", {
              kind: "close-issue",
            }),
          );
          await system.run(
            system.engine.dispatch({
              type: "thread.settle",
              commandId: CommandId.make("cmd-stopped-dora-settle"),
              threadId,
              expectedSnapshotSequence: closeIssue.sequence,
            }),
          );
          const afterClosure = Option.getOrThrow(await system.readThread(threadId));
          expect(afterClosure).toMatchObject({
            settledOverride: "settled",
            latestTurn: {
              turnId,
              state: "completed",
              completedAt,
              assistantMessageId,
            },
            session: {
              status: "stopped",
              providerName: "dora",
              providerInstanceId,
              providerSessionId,
              activeTurnId: null,
              lastError: null,
            },
          });
          await system.run(
            appendActivity("cmd-stopped-dora-repeat-verification", "dora.verification", {}),
          );
          await system.run(
            system.engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-stopped-dora-revoked"),
              threadId,
              createdAt: "2026-09-08T10:46:31.000Z",
              session: {
                threadId,
                status: "error",
                providerName: "dora",
                providerInstanceId,
                providerSessionId,
                runtimeMode: "auto",
                activeTurnId: null,
                lastError: "provider failed after closure",
                updatedAt: "2026-09-08T10:46:31.000Z",
              },
            }),
          );
          await expect(
            system.run(
              appendActivity("cmd-stopped-dora-revoked-activity", "dora.verification", {}),
            ),
          ).rejects.toThrow("clean completed stopped Dora session");
        } finally {
          await system.dispose();
          await NodeFSP.rm(directory, { recursive: true, force: true });
        }
      }),
  );

  effectIt.effect.each([
    {
      name: "non-Dora",
      providerName: "codex",
      providerSessionId: "other-session",
      status: "ready",
    },
    { name: "absent", providerName: null, providerSessionId: undefined, status: undefined },
    {
      name: "stopped",
      providerName: "dora",
      providerSessionId: "dora-session-stopped",
      status: "stopped",
    },
  ] as const)(
    "rejects a trusted Dora capability for a $name session",
    ({ providerName, providerSessionId, status }) =>
      Effect.promise(async () => {
        const system = await createOrchestrationSystem();
        const projectId = ProjectId.make(
          `project-dora-${providerName ?? "absent"}-${status ?? "none"}`,
        );
        const threadId = ThreadId.make(
          `thread-dora-${providerName ?? "absent"}-${status ?? "none"}`,
        );
        const providerInstanceId = ProviderInstanceId.make("dora");
        const createdAt = now();
        try {
          await system.run(
            system.engine.dispatch({
              type: "project.create",
              commandId: CommandId.make(`cmd-${threadId}-project`),
              projectId,
              title: "Project",
              workspaceRoot: `/tmp/${threadId}`,
              createdAt,
            }),
          );
          await system.run(
            system.engine.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`cmd-${threadId}-thread`),
              threadId,
              projectId,
              title: "Thread",
              modelSelection: { instanceId: providerInstanceId, model: "dora" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt,
            }),
          );
          if (status !== undefined && providerName !== null && providerSessionId !== undefined) {
            await system.run(
              system.engine.dispatch({
                type: "thread.session.set",
                commandId: CommandId.make(`cmd-${threadId}-session`),
                threadId,
                createdAt,
                session: {
                  threadId,
                  status,
                  providerName,
                  providerInstanceId,
                  providerSessionId,
                  runtimeMode: "full-access",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: createdAt,
                },
              }),
            );
          }
          await expect(
            system.run(
              system.engine.dispatch(
                {
                  type: "thread.activity.append",
                  commandId: CommandId.make(`cmd-${threadId}-activity`),
                  threadId,
                  providerInstanceId,
                  providerSessionId: ProviderSessionId.make(providerSessionId ?? "missing"),
                  activity: {
                    id: EventId.make(`activity-${threadId}`),
                    tone: "info",
                    kind: "dora.plan",
                    summary: "Plan",
                    payload: {},
                    turnId: null,
                    createdAt,
                  },
                  createdAt,
                },
                {
                  doraActivityCapability: createAuthenticatedDoraActivityCapability({
                    threadId,
                    providerInstanceId,
                    providerSessionId: ProviderSessionId.make(providerSessionId ?? "missing"),
                  }),
                },
              ),
            ),
          ).rejects.toThrow("active bound Dora session");
        } finally {
          await system.dispose();
        }
      }),
  );
});
