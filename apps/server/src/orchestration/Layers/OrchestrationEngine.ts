import type {
  OrchestrationClientOrigin,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { OrchestrationCommand } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  isOrchestrationCommandRejection,
  OrchestrationCommandIdConflictError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  type OrchestrationDispatchError,
  type OrchestrationProjectorDecodeError,
} from "../Errors.ts";
import { decideOrchestrationCommand } from "../decider.ts";
import { validateDoraActivity } from "../Normalizer.ts";
import {
  isAuthenticatedDoraActivityCapability,
  type AuthenticatedDoraActivityCapability,
} from "../DoraActivityAuthorization.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ThreadBackgroundLivenessService } from "../ThreadBackgroundLiveness.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isOrchestrationCommandIdConflictError = Schema.is(OrchestrationCommandIdConflictError);

interface CommandEnvelope {
  command: OrchestrationCommand;
  origin: OrchestrationClientOrigin | undefined;
  doraActivityCapability: AuthenticatedDoraActivityCapability | undefined;
  result: Deferred.Deferred<{ sequence: number }, OrchestrationDispatchError>;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const threadBackgroundLiveness = yield* ThreadBackgroundLivenessService;
  const crypto = yield* Crypto.Crypto;
  const getPendingTurnStart = SqlSchema.findOneOption({
    Request: Schema.Struct({ threadId: Schema.String }),
    Result: Schema.Struct({ pending: Schema.Literal(1) }),
    execute: ({ threadId }) => sql`
      SELECT 1 AS "pending"
      FROM projection_turns
      WHERE thread_id = ${threadId}
        AND turn_id IS NULL
        AND state = 'pending'
        AND pending_message_id IS NOT NULL
        AND checkpoint_turn_count IS NULL
      LIMIT 1
    `,
  });

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

  const authorizeDoraActivity = (
    command: OrchestrationCommand,
    capability: AuthenticatedDoraActivityCapability | undefined,
    receivedAt: string,
  ): Effect.Effect<OrchestrationCommand, OrchestrationDispatchError> => {
    if (command.type !== "thread.activity.append" || !command.activity.kind.startsWith("dora.")) {
      return Effect.succeed(command);
    }
    if (!isAuthenticatedDoraActivityCapability(capability)) {
      return Effect.fail(
        new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Dora activity append requires an authenticated control-plane capability.",
        }),
      );
    }
    if (
      !("providerInstanceId" in command) ||
      !("providerSessionId" in command) ||
      command.providerInstanceId !== capability.providerInstanceId ||
      command.providerSessionId !== capability.providerSessionId
    ) {
      return Effect.fail(
        new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Dora activity does not carry the authenticated provider session binding.",
        }),
      );
    }
    const validationError = validateDoraActivity(command.activity);
    if (validationError !== undefined) {
      return Effect.fail(
        new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Invalid Dora activity: ${validationError}.`,
        }),
      );
    }

    const hasActiveBoundDoraSession = (readModel: OrchestrationReadModel): boolean => {
      const thread = readModel.threads.find((candidate) => candidate.id === command.threadId);
      const session = thread?.session;
      return (
        thread !== undefined &&
        thread.id === command.threadId &&
        thread.deletedAt === null &&
        session != null &&
        session.threadId === command.threadId &&
        session.providerName === "dora" &&
        (session.status === "running" || session.status === "ready") &&
        session.providerInstanceId === capability.providerInstanceId &&
        session.providerSessionId === capability.providerSessionId &&
        capability.threadId === command.threadId
      );
    };
    const stampAtReceipt = (): OrchestrationCommand => ({
      // The authenticated control-plane path cannot preserve a caller-controlled
      // clock in retained activity data. Stamp both command and activity at engine receipt time.
      ...command,
      createdAt: receivedAt,
      activity: { ...command.activity, createdAt: receivedAt },
    });

    if (hasActiveBoundDoraSession(commandReadModel)) {
      return Effect.succeed(stampAtReceipt());
    }

    // Provider ingestion can append activity after the session projection is
    // durable but before this worker receives the corresponding session event.
    // Only this otherwise-rejected Dora activity path may refresh the command
    // model, and a lower-sequence projection can never replace newer state.
    return projectionSnapshotQuery.getCommandReadModel().pipe(
      Effect.mapError(
        (cause) =>
          new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: "Dora activity does not match an active bound Dora session.",
            cause,
          }),
      ),
      Effect.flatMap((persistedReadModel) => {
        if (persistedReadModel.snapshotSequence >= commandReadModel.snapshotSequence) {
          commandReadModel = persistedReadModel;
        }
        return hasActiveBoundDoraSession(commandReadModel)
          ? Effect.succeed(stampAtReceipt())
          : Effect.fail(
              new OrchestrationCommandInvariantError({
                commandType: command.type,
                detail: "Dora activity does not match an active bound Dora session.",
              }),
            );
      }),
    );
  };

  const projectEventsOntoReadModel = (
    baseReadModel: OrchestrationReadModel,
    events: ReadonlyArray<OrchestrationEvent>,
  ): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError, never> =>
    Effect.gen(function* () {
      let nextReadModel = baseReadModel;
      for (const event of events) {
        nextReadModel = yield* projectEvent(nextReadModel, event);
      }
      return nextReadModel;
    });

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const dispatchStartSequence = commandReadModel.snapshotSequence;
    let processingStartedAtMs = 0;
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      commandReadModel = yield* projectEventsOntoReadModel(commandReadModel, persistedEvents);

      for (const persistedEvent of persistedEvents) {
        yield* PubSub.publish(eventPubSub, persistedEvent);
      }
    });

    return Effect.exit(
      Effect.gen(function* () {
        processingStartedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const command = yield* authorizeDoraActivity(
          envelope.command,
          envelope.doraActivityCapability,
          yield* nowIso,
        );

        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        if (Option.isSome(existingReceipt)) {
          // A receipt only proves this exact command was handled. Replaying it
          // for a command aimed at another aggregate would report success for
          // work that never happened.
          if (
            existingReceipt.value.aggregateKind !== aggregateRef.aggregateKind ||
            existingReceipt.value.aggregateId !== aggregateRef.aggregateId
          ) {
            return yield* new OrchestrationCommandIdConflictError({
              commandId: envelope.command.commandId,
              receiptAggregateKind: existingReceipt.value.aggregateKind,
              receiptAggregateId: existingReceipt.value.aggregateId,
              commandAggregateKind: aggregateRef.aggregateKind,
              commandAggregateId: aggregateRef.aggregateId,
            });
          }
          if (existingReceipt.value.status === "accepted") {
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        const guardedThreadOperation =
          envelope.command.type === "thread.auto-settle"
            ? {
                kind: "settlement" as const,
                threadId: envelope.command.threadId,
                expectedSnapshotSequence: envelope.command.snapshotSequence,
              }
            : envelope.command.type === "thread.settle" &&
                envelope.command.expectedSnapshotSequence !== undefined
              ? {
                  kind: "settlement" as const,
                  threadId: envelope.command.threadId,
                  expectedSnapshotSequence: envelope.command.expectedSnapshotSequence,
                }
              : envelope.command.type === "thread.turn.start" &&
                  envelope.command.expectedSnapshotSequence !== undefined
                ? {
                    kind: "turn-start" as const,
                    threadId: envelope.command.threadId,
                    expectedSnapshotSequence: envelope.command.expectedSnapshotSequence,
                  }
                : undefined;
        if (
          guardedThreadOperation !== undefined &&
          guardedThreadOperation.expectedSnapshotSequence > commandReadModel.snapshotSequence
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${guardedThreadOperation.threadId} guarded snapshot is ahead of the authoritative sequence`,
          });
        }

        if (
          guardedThreadOperation !== undefined &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: guardedThreadOperation.threadId,
            sequenceExclusive: guardedThreadOperation.expectedSnapshotSequence,
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${guardedThreadOperation.threadId} changed before guarded ${guardedThreadOperation.kind}`,
          });
        }

        if (
          guardedThreadOperation !== undefined &&
          threadBackgroundLiveness.getThreadBackgroundLiveness(guardedThreadOperation.threadId) !==
            null
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${guardedThreadOperation.threadId} has live background work`,
          });
        }

        if (guardedThreadOperation?.kind === "turn-start") {
          if (envelope.command.type !== "thread.turn.start") {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: "Guarded turn start command type mismatch.",
            });
          }
          if (envelope.command.bootstrap !== undefined) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} guarded turn start cannot bootstrap a thread`,
            });
          }
          const thread = commandReadModel.threads.find(
            (candidate) => candidate.id === guardedThreadOperation.threadId,
          );
          if (thread === undefined || thread.deletedAt !== null || thread.archivedAt !== null) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} does not have an existing active binding`,
            });
          }
          const threadShell = yield* projectionSnapshotQuery
            .getThreadShellById(guardedThreadOperation.threadId)
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationCommandInvariantError({
                    commandType: envelope.command.type,
                    detail: `thread ${guardedThreadOperation.threadId} terminal state is unavailable`,
                    cause,
                  }),
              ),
            );
          if (Option.isNone(threadShell)) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} does not have an existing active binding`,
            });
          }
          const terminalThread = threadShell.value;
          const terminalSession = terminalThread.session;
          const terminalTurn = terminalThread.latestTurn;
          if (
            terminalSession === null ||
            terminalSession.status === "starting" ||
            terminalSession.status === "running" ||
            terminalSession.activeTurnId !== null
          ) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} has an active session or turn`,
            });
          }
          if (
            terminalTurn === null ||
            terminalTurn.state === "running" ||
            terminalTurn.completedAt === null
          ) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} does not have a completed terminal turn`,
            });
          }
          if (
            terminalSession.status !== "error" &&
            terminalSession.status !== "stopped" &&
            terminalSession.status !== "interrupted" &&
            !(terminalSession.status === "ready" && terminalTurn.state === "completed")
          ) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} does not have a terminal session binding`,
            });
          }
          if (terminalThread.hasPendingApprovals || terminalThread.hasPendingUserInput) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} has pending approval or user input`,
            });
          }
          const pendingTurnStart = yield* getPendingTurnStart({
            threadId: guardedThreadOperation.threadId,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: `thread ${guardedThreadOperation.threadId} pending turn state is unavailable`,
                  cause,
                }),
            ),
          );
          if (Option.isSome(pendingTurnStart)) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: envelope.command.type,
              detail: `thread ${guardedThreadOperation.threadId} has a queued turn start`,
            });
          }
        }

        // Command snapshots omit activities at startup and cap them while running.
        // Read this request's durable state before deciding how to send the answer.
        const userInputActivity =
          envelope.command.type === "thread.user-input.respond"
            ? yield* projectionSnapshotQuery.getUserInputActivity(envelope.command)
            : Option.none();
        const eventBase = yield* decideOrchestrationCommand({
          command,
          readModel: commandReadModel,
          ...(Option.isSome(userInputActivity)
            ? { userInputActivity: userInputActivity.value }
            : {}),
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            isOrchestrationCommandRejection(cause)
              ? cause
              : new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Failed to generate an event identifier.",
                  cause,
                }),
          ),
        );
        const plannedEvents = Array.isArray(eventBase) ? eventBase : [eventBase];
        // Stamp the dispatching client's origin onto every event the command
        // produced. The decider stays pure; attribution is an engine concern.
        const eventBases =
          envelope.origin === undefined
            ? plannedEvents
            : plannedEvents.map((planned) => ({
                ...planned,
                metadata: { ...planned.metadata, origin: envelope.origin },
              }));
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const committedEvents: OrchestrationEvent[] = [];
              const attachmentCleanups: Effect.Effect<void>[] = [];
              let nextCommandReadModel = commandReadModel;

              for (const nextEvent of eventBases) {
                const savedEvent = yield* eventStore.append(nextEvent);
                nextCommandReadModel = yield* projectEvent(nextCommandReadModel, savedEvent);
                const cleanup = yield* projectionPipeline.projectEventDeferred(savedEvent);
                attachmentCleanups.push(cleanup);
                committedEvents.push(savedEvent);
              }

              const lastSavedEvent = committedEvents.at(-1) ?? null;
              if (lastSavedEvent === null) {
                return yield* new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Command produced no events.",
                });
              }

              yield* commandReceiptRepository.upsert({
                commandId: envelope.command.commandId,
                aggregateKind: lastSavedEvent.aggregateKind,
                aggregateId: lastSavedEvent.aggregateId,
                acceptedAt: lastSavedEvent.occurredAt,
                resultSequence: lastSavedEvent.sequence,
                status: "accepted",
                error: null,
              });

              return {
                committedEvents,
                attachmentCleanups,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
              } as const;
            }),
          )
          .pipe(
            Effect.catchTag("SqlError", (sqlError) =>
              Effect.fail(
                toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
              ),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        for (const cleanup of committedCommand.attachmentCleanups) {
          yield* cleanup;
        }
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(Effect.withSpan(`orchestration.command.${envelope.command.type}`)),
    ).pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            yield* Deferred.succeed(envelope.result, exit.value);
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (
            !isOrchestrationCommandPreviouslyRejectedError(error) &&
            !isOrchestrationCommandIdConflictError(error)
          ) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: commandReadModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (isOrchestrationCommandRejection(error)) {
              yield* commandReceiptRepository
                .upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: yield* nowIso,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.catch(() => Effect.void));
            }
          }

          yield* Deferred.fail(envelope.result, error);
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();

  const worker = Effect.forever(Queue.take(commandQueue).pipe(Effect.flatMap(processEnvelope)));
  yield* Effect.forkScoped(worker);
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: commandReadModel.snapshotSequence }),
  );

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive, limit) =>
    eventStore.readFromSequence(fromSequenceExclusive, limit);

  const dispatch: OrchestrationEngineShape["dispatch"] = (command, options) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      yield* Queue.offer(commandQueue, {
        command,
        origin: options?.origin,
        doraActivityCapability: options?.doraActivityCapability,
        result,
        startedAtMs: yield* Clock.currentTimeMillis,
      });
      return yield* Deferred.await(result);
    });

  return {
    readEvents,
    dispatch,
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
    // The command read model's snapshotSequence tracks the latest committed
    // event sequence (updated on the worker fiber). A plain property read is a
    // consistent, committed value — reassignment of `commandReadModel` is
    // atomic on the single-threaded event loop.
    latestSequence: Effect.sync(() => commandReadModel.snapshotSequence),
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
