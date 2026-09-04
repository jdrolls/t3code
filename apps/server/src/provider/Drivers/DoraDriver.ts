/** DoraDriver — registers the credential-free Dora JSONL runtime. */
import {
  DoraSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeDoraAdapter } from "../Layers/DoraAdapter.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { buildServerProvider } from "../providerSnapshot.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";

const DRIVER_KIND = ProviderDriverKind.make("dora");
const decodeDoraSettings = Schema.decodeSync(DoraSettings);

const unsupportedTextGeneration = (
  operation: "generateCommitMessage" | "generatePrContent" | "generateBranchName" | "generateThreadTitle",
) =>
  Effect.fail(new TextGenerationError({ operation, detail: "Dora does not implement text generation." }));

/** Dora needs no account secret or server-side credential service. */
export type DoraDriverEnv = never;

export const DoraDriver: ProviderDriver<DoraSettings, DoraDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: { displayName: "Dora", supportsMultipleInstances: true },
  configSchema: DoraSettings,
  defaultConfig: (): DoraSettings => decodeDoraSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const continuationIdentity = defaultProviderContinuationIdentity({ driverKind: DRIVER_KIND, instanceId });
      const effectiveConfig = { ...config, enabled } satisfies DoraSettings;
      const processEnv = environment?.reduce<NodeJS.ProcessEnv>((variables, { name, value }) => {
        if (typeof value === "string") variables[name] = value;
        return variables;
      }, {});
      // Dora receives a strict environment allowlist inside the adapter; this
      // per-instance input is only a source for safe process-locale variables.
      const adapter = yield* makeDoraAdapter(effectiveConfig, {
        instanceId,
        environment: processEnv,
      });
      const checkedAt = DateTime.formatIso(yield* DateTime.now);
      const snapshot: ServerProvider = {
        ...buildServerProvider({
          presentation: { displayName: "Dora", showInteractionModeToggle: false },
          enabled,
          checkedAt,
          models: [],
          probe: {
            installed: enabled,
            version: null,
            status: enabled ? "ready" : "warning",
            auth: { status: "unknown" },
            ...(enabled
              ? { message: "Dora uses the local credential-free JSONL runtime." }
              : { message: "Dora is disabled in T3 Code settings." }),
          },
        }),
        instanceId,
        driver: DRIVER_KIND,
        ...(displayName ? { displayName } : {}),
        ...(accentColor ? { accentColor } : {}),
        continuation: { groupKey: continuationIdentity.continuationKey },
      };
      const textGeneration: TextGeneration.TextGeneration["Service"] = {
        generateCommitMessage: () => unsupportedTextGeneration("generateCommitMessage"),
        generatePrContent: () => unsupportedTextGeneration("generatePrContent"),
        generateBranchName: () => unsupportedTextGeneration("generateBranchName"),
        generateThreadTitle: () => unsupportedTextGeneration("generateThreadTitle"),
      };
      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot: {
          maintenanceCapabilities: makeManualOnlyProviderMaintenanceCapabilities({ provider: DRIVER_KIND, packageName: null }),
          getSnapshot: Effect.succeed(snapshot),
          refresh: Effect.succeed(snapshot),
          streamChanges: Stream.empty,
        },
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }).pipe(
      Effect.mapError((cause) =>
        Schema.is(ProviderDriverError)(cause)
          ? cause
          : new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Dora provider instance.",
              cause,
            }),
      ),
    ),
};
