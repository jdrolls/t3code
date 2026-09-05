import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import {
  type ClientOrchestrationCommand,
  DORA_CLIENT_ACTIVITY_MAX_ID_CHARS,
  DORA_CLIENT_ACTIVITY_MAX_SUMMARY_CHARS,
  type IsoDateTime,
  type OrchestrationCommand,
  OrchestrationDispatchCommandError,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
} from "@t3tools/contracts";

import {
  createAttachmentId,
  planAttachmentClaim,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  parseThreadSegmentFromAttachmentId,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import { ServerConfig } from "../config.ts";
import { parseBase64DataUrl } from "../imageMime.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";

export const canonicalizeClientCommandTimestamps = (
  command: ClientOrchestrationCommand,
  receivedAt: IsoDateTime,
): ClientOrchestrationCommand => {
  const canonicalCommand =
    "createdAt" in command
      ? {
          ...command,
          createdAt: receivedAt,
        }
      : command;

  if (canonicalCommand.type === "thread.activity.append") {
    return {
      ...canonicalCommand,
      activity: {
        ...canonicalCommand.activity,
        createdAt: receivedAt,
      },
    };
  }

  if (canonicalCommand.type !== "thread.turn.start" || !canonicalCommand.bootstrap?.createThread) {
    return canonicalCommand;
  }

  return {
    ...canonicalCommand,
    bootstrap: {
      ...canonicalCommand.bootstrap,
      createThread: {
        ...canonicalCommand.bootstrap.createThread,
        createdAt: receivedAt,
      },
    },
  };
};

const DORA_ACTIVITY_KINDS = new Set([
  "dora.plan",
  "dora.replan",
  "dora.verification",
  "dora.side-effect",
]);
const DORA_ACTIVITY_MAX_PAYLOAD_BYTES = 16 * 1024;
const DORA_ACTIVITY_MAX_PAYLOAD_DEPTH = 5;
const DORA_ACTIVITY_MAX_PAYLOAD_NODES = 200;
const DORA_ACTIVITY_MAX_RECORD_KEYS = 50;
const DORA_ACTIVITY_MAX_ARRAY_ITEMS = 50;
const DORA_ACTIVITY_MAX_KEY_CHARS = 128;
const DORA_ACTIVITY_MAX_STRING_CHARS = 4_096;
const UNSAFE_JSON_KEYS = new Set(["__proto__", "constructor", "prototype"]);

/**
 * Trusted Dora payloads become retained projection data. Accept only a small,
 * data-only JSON tree so the engine's provider path cannot smuggle executable
 * values, prototype-sensitive keys, or an unbounded retained object graph.
 */
function validateDoraActivityPayload(payload: unknown): string | undefined {
  const seen = new WeakSet<object>();
  let nodeCount = 0;

  const visit = (value: unknown, depth: number): string | undefined => {
    if (depth > DORA_ACTIVITY_MAX_PAYLOAD_DEPTH) {
      return `payload exceeds maximum depth of ${DORA_ACTIVITY_MAX_PAYLOAD_DEPTH}`;
    }
    nodeCount += 1;
    if (nodeCount > DORA_ACTIVITY_MAX_PAYLOAD_NODES) {
      return `payload exceeds maximum node count of ${DORA_ACTIVITY_MAX_PAYLOAD_NODES}`;
    }

    if (value === null || typeof value === "boolean") return undefined;
    if (typeof value === "string") {
      return value.length <= DORA_ACTIVITY_MAX_STRING_CHARS
        ? undefined
        : `payload string exceeds ${DORA_ACTIVITY_MAX_STRING_CHARS} characters`;
    }
    if (typeof value === "number") {
      return Number.isFinite(value) ? undefined : "payload contains a non-finite number";
    }
    if (typeof value !== "object") return "payload contains a non-JSON value";
    if (seen.has(value)) return "payload contains a repeated or cyclic reference";
    seen.add(value);

    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length > 0) {
        return "payload arrays must be plain data-only arrays";
      }
      if (value.length > DORA_ACTIVITY_MAX_ARRAY_ITEMS) {
        return `payload array exceeds ${DORA_ACTIVITY_MAX_ARRAY_ITEMS} items`;
      }
      const propertyNames = Object.getOwnPropertyNames(value);
      if (propertyNames.length !== value.length + 1 || !propertyNames.includes("length")) {
        return "payload arrays must be dense data-only arrays";
      }
      for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor?.enumerable || !("value" in descriptor)) {
          return "payload arrays must be dense data-only arrays";
        }
        const error = visit(descriptor.value, depth + 1);
        if (error !== undefined) return error;
      }
      return undefined;
    }

    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return "payload records must be plain objects";
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      return "payload records cannot contain symbol keys";
    }
    const propertyNames = Object.getOwnPropertyNames(value);
    if (propertyNames.length > DORA_ACTIVITY_MAX_RECORD_KEYS) {
      return `payload record exceeds ${DORA_ACTIVITY_MAX_RECORD_KEYS} keys`;
    }
    for (const key of propertyNames) {
      if (
        key.length > DORA_ACTIVITY_MAX_KEY_CHARS ||
        UNSAFE_JSON_KEYS.has(key)
      ) {
        return "payload contains an unsafe or oversized key";
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !("value" in descriptor)) {
        return "payload records must contain enumerable data properties only";
      }
      const error = visit(descriptor.value, depth + 1);
      if (error !== undefined) return error;
    }
    return undefined;
  };

  try {
    const rootIsRecord =
      typeof payload === "object" &&
      payload !== null &&
      !Array.isArray(payload) &&
      (Object.getPrototypeOf(payload) === Object.prototype || Object.getPrototypeOf(payload) === null);
    if (!rootIsRecord) return "payload must be a JSON-safe record";

    const structuralError = visit(payload, 0);
    if (structuralError !== undefined) return structuralError;
    const serialized = JSON.stringify(payload);
    if (serialized === undefined || Buffer.byteLength(serialized, "utf8") > DORA_ACTIVITY_MAX_PAYLOAD_BYTES) {
      return `payload exceeds ${DORA_ACTIVITY_MAX_PAYLOAD_BYTES} bytes`;
    }
    return undefined;
  } catch {
    return "payload must be a JSON-safe record";
  }
}

export interface DoraActivityCandidate {
  readonly id: unknown;
  readonly tone: unknown;
  readonly kind: unknown;
  readonly summary: unknown;
  readonly payload: unknown;
  readonly turnId: unknown;
}

/** Shared by the RPC normalizer and engine-side trusted Dora dispatch. */
export function validateDoraActivity(activity: DoraActivityCandidate): string | undefined {
  if (
    typeof activity.id !== "string" ||
    activity.id.length === 0 ||
    activity.id.length > DORA_CLIENT_ACTIVITY_MAX_ID_CHARS
  ) {
    return "activity id is invalid or oversized";
  }
  if (typeof activity.kind !== "string" || !DORA_ACTIVITY_KINDS.has(activity.kind)) {
    return "activity kind is not a Dora projection kind";
  }
  if (activity.tone !== "info") return "activity tone must be info";
  if (activity.turnId !== null) return "activity turnId must be null";
  if (
    typeof activity.summary !== "string" ||
    activity.summary.trim().length === 0 ||
    activity.summary.length > DORA_CLIENT_ACTIVITY_MAX_SUMMARY_CHARS
  ) {
    return "activity summary is invalid or oversized";
  }
  const payloadValidationError = validateDoraActivityPayload(activity.payload);
  if (payloadValidationError !== undefined) return payloadValidationError;
  if (containsDoraSecret(activity.summary) || containsDoraSecret(activity.payload)) {
    return "activity contains secret-bearing content";
  }
  return undefined;
}

/** Reject common credential forms before a Dora projection becomes retained data. */
export function containsDoraSecret(value: unknown, seen = new Set<unknown>()): boolean {
  if (typeof value === "string") {
    return /(?:api[_-]?key|authorization|secret|password|token)\s*[:=]|bearer\s+[a-z0-9._~+/=-]{8,}/iu.test(
      value,
    );
  }
  if (typeof value !== "object" || value === null) return false;
  if (seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some((entry) => containsDoraSecret(entry, seen));
  return Object.entries(value).some(
    ([key, entry]) =>
      /(?:api[_-]?key|authorization|secret|password|credential|access[_-]?token|refresh[_-]?token|token)/iu.test(
        key,
      ) || containsDoraSecret(entry, seen),
  );
}

const removeClaimedAttachmentPaths = Effect.fn("Normalizer.removeClaimedAttachmentPaths")(
  function* (attachmentPaths: ReadonlyArray<string>) {
    if (attachmentPaths.length === 0) {
      return;
    }
    const fileSystem = yield* FileSystem.FileSystem;
    yield* Effect.forEach(
      attachmentPaths,
      (attachmentPath) =>
        fileSystem.remove(attachmentPath, { force: true }).pipe(
          Effect.tapError((cause) =>
            Effect.logWarning("Failed to remove an unclaimed attachment copy.", {
              attachmentPath,
              cause,
            }),
          ),
          Effect.orElseSucceed(() => undefined),
        ),
      { concurrency: 1 },
    );
  },
);

export const normalizeDispatchCommand = (command: ClientOrchestrationCommand) =>
  Effect.gen(function* () {
    const receivedAt = DateTime.formatIso(yield* DateTime.now);
    const canonicalCommand = canonicalizeClientCommandTimestamps(command, receivedAt);

    if (canonicalCommand.type === "thread.activity.append") {
      const validationError = validateDoraActivity(canonicalCommand.activity);
      if (validationError !== undefined) {
        return yield* new OrchestrationDispatchCommandError({
          message: `Invalid Dora activity: ${validationError}.`,
        });
      }
      // The WebSocket handler separately requires the Dora control-plane scope
      // and supplies its non-serializable capability. Keeping the command
      // data-only here means direct engine callers cannot recreate that proof.
      return canonicalCommand as OrchestrationCommand;
    }

    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;

    const normalizeProjectWorkspaceRoot = (workspaceRoot: string) =>
      workspacePaths.normalizeWorkspaceRoot(workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationDispatchCommandError({
              message: cause.message,
            }),
        ),
      );

    const normalizeProjectWorkspaceRootForCreate = (
      workspaceRoot: string,
      createIfMissing: boolean | undefined,
    ) =>
      workspacePaths
        .normalizeWorkspaceRoot(workspaceRoot, {
          createIfMissing: createIfMissing === true,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationDispatchCommandError({
                message: cause.message,
              }),
          ),
        );

    if (canonicalCommand.type === "project.create") {
      return {
        ...canonicalCommand,
        workspaceRoot: yield* normalizeProjectWorkspaceRootForCreate(
          canonicalCommand.workspaceRoot,
          canonicalCommand.createWorkspaceRootIfMissing,
        ),
        createWorkspaceRootIfMissing: canonicalCommand.createWorkspaceRootIfMissing === true,
      } satisfies OrchestrationCommand;
    }

    if (
      canonicalCommand.type === "project.meta.update" &&
      canonicalCommand.workspaceRoot !== undefined
    ) {
      return {
        ...canonicalCommand,
        workspaceRoot: yield* normalizeProjectWorkspaceRoot(canonicalCommand.workspaceRoot),
      } satisfies OrchestrationCommand;
    }

    if (canonicalCommand.type !== "thread.turn.start") {
      return canonicalCommand as OrchestrationCommand;
    }

    const claimedAttachmentPaths: string[] = [];
    const normalizedAttachments = yield* Effect.forEach(
      canonicalCommand.message.attachments,
      (attachment) =>
        Effect.gen(function* () {
          if (!("dataUrl" in attachment)) {
            const claim = planAttachmentClaim({
              attachmentsDir: serverConfig.attachmentsDir,
              threadId: canonicalCommand.threadId,
              attachmentId: attachment.id,
            });
            if (!claim.ok) {
              return yield* new OrchestrationDispatchCommandError({
                message: `Attachment '${attachment.name}' cannot be sent: ${claim.reason}.`,
              });
            }

            const info = yield* fileSystem.stat(claim.currentPath).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationDispatchCommandError({
                    message: `Attachment '${attachment.name}' cannot be sent: attachment not found.`,
                    cause,
                  }),
              ),
            );
            if (Number(info.size) !== attachment.sizeBytes) {
              return yield* new OrchestrationDispatchCommandError({
                message: `Attachment '${attachment.name}' cannot be sent: stored size does not match.`,
              });
            }

            const normalizedAttachment = {
              ...attachment,
              id: claim.finalId,
              mimeType: attachment.mimeType.toLowerCase(),
            };
            const expectedPath = resolveAttachmentPath({
              attachmentsDir: serverConfig.attachmentsDir,
              attachment: normalizedAttachment,
            });
            if (expectedPath !== claim.finalPath) {
              return yield* new OrchestrationDispatchCommandError({
                message: `Attachment '${attachment.name}' cannot be sent: attachment type does not match the upload.`,
              });
            }

            // Keep the pending copy until the turn succeeds. A failed thread
            // bootstrap can then retry with a fresh thread id. A copy, not a
            // hard link: an agent editing the delivered file in place must not
            // mutate the retry source.
            yield* fileSystem.copyFile(claim.currentPath, claim.finalPath).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationDispatchCommandError({
                    message: `Failed to claim attachment '${attachment.name}' for this thread.`,
                    cause,
                  }),
              ),
            );
            claimedAttachmentPaths.push(claim.finalPath);

            return normalizedAttachment;
          }

          const parsed = parseBase64DataUrl(attachment.dataUrl);
          if (!parsed || !parsed.mimeType.startsWith("image/")) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Invalid image attachment payload for '${attachment.name}'.`,
            });
          }

          const bytes = Buffer.from(parsed.base64, "base64");
          if (bytes.byteLength === 0 || bytes.byteLength > PROVIDER_SEND_TURN_MAX_IMAGE_BYTES) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Image attachment '${attachment.name}' is empty or too large.`,
            });
          }

          const attachmentId = createAttachmentId(canonicalCommand.threadId);
          if (!attachmentId) {
            return yield* new OrchestrationDispatchCommandError({
              message: "Failed to create a safe attachment id.",
            });
          }

          const persistedAttachment = {
            type: "image" as const,
            id: attachmentId,
            name: attachment.name,
            mimeType: parsed.mimeType.toLowerCase(),
            sizeBytes: bytes.byteLength,
          };

          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment: persistedAttachment,
          });
          if (!attachmentPath) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Failed to resolve persisted path for '${attachment.name}'.`,
            });
          }

          yield* fileSystem.makeDirectory(path.dirname(attachmentPath), { recursive: true }).pipe(
            Effect.mapError(
              () =>
                new OrchestrationDispatchCommandError({
                  message: `Failed to create attachment directory for '${attachment.name}'.`,
                }),
            ),
          );
          yield* fileSystem.writeFile(attachmentPath, bytes).pipe(
            Effect.mapError(
              () =>
                new OrchestrationDispatchCommandError({
                  message: `Failed to persist attachment '${attachment.name}'.`,
                }),
            ),
          );

          return persistedAttachment;
        }),
      { concurrency: 1 },
    ).pipe(Effect.tapError(() => removeClaimedAttachmentPaths(claimedAttachmentPaths)));

    return {
      ...canonicalCommand,
      message: {
        ...canonicalCommand.message,
        attachments: normalizedAttachments,
      },
    } satisfies OrchestrationCommand;
  });

export const cleanupFailedUploadedAttachments = Effect.fn(
  "Normalizer.cleanupFailedUploadedAttachments",
)(function* (command: ClientOrchestrationCommand, normalizedCommand: OrchestrationCommand) {
  if (command.type !== "thread.turn.start" || normalizedCommand.type !== "thread.turn.start") {
    return;
  }

  const serverConfig = yield* ServerConfig;
  const claimedPaths: string[] = [];
  for (const [index, attachment] of normalizedCommand.message.attachments.entries()) {
    const original = command.message.attachments[index];
    if (
      !original ||
      "dataUrl" in original ||
      parseThreadSegmentFromAttachmentId(original.id) !== PENDING_ATTACHMENT_THREAD_SEGMENT
    ) {
      continue;
    }

    const claimedPath = resolveAttachmentPath({
      attachmentsDir: serverConfig.attachmentsDir,
      attachment,
    });
    if (claimedPath) {
      claimedPaths.push(claimedPath);
    }
  }
  yield* removeClaimedAttachmentPaths(claimedPaths);
});
