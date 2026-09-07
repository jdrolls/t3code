import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import serverPackageJson from "../apps/server/package.json" with { type: "json" };
import {
  assertCanonicalStagingRoot,
  FORK_REPOSITORY_URL,
  RELEASE_VERSION,
  resolvePackageRoot,
  validateSourcePackage,
} from "./stage-t3-runtime.mjs";
import { SERVICE_LAUNCHER_PROTOCOL } from "./lib/service-launcher-protocol.mjs";

const scriptsDirectory = NodeURL.fileURLToPath(new URL(".", import.meta.url));
const repositoryRoot = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const stageRuntimeScriptPath = NodeURL.fileURLToPath(
  new URL("./stage-t3-runtime.mjs", import.meta.url),
);
const stageRuntimeInvocationCwds = [repositoryRoot, scriptsDirectory] as const;

const SourcePackageJson = Schema.fromJsonString(Schema.Struct({ name: Schema.String }));
const PackageMetadata = Schema.Struct({
  name: Schema.String,
  exports: Schema.Struct({}),
});
const PackageMetadataJson = Schema.fromJsonString(PackageMetadata);
const PackageIdentityJson = Schema.fromJsonString(
  Schema.Struct({ name: Schema.String, version: Schema.String }),
);
const StageResultJson = Schema.fromJsonString(
  Schema.Struct({
    versionRoot: Schema.String,
    entryPath: Schema.String,
    sentinelPath: Schema.String,
  }),
);
const PreflightResultJson = Schema.fromJsonString(
  Schema.Struct({
    status: Schema.String,
    version: Schema.String,
    launcherProtocol: Schema.Number,
  }),
);

const encodeSourcePackage = Schema.encodeEffect(SourcePackageJson);
const encodePackageMetadata = Schema.encodeEffect(PackageMetadataJson);
const decodePackageIdentity = Schema.decodeUnknownEffect(PackageIdentityJson);
const decodeStageResult = Schema.decodeUnknownEffect(StageResultJson);
const decodePreflightResult = Schema.decodeUnknownEffect(PreflightResultJson);

const withTemporaryPackage = <A, E, R>(test: (packageRoot: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const temporaryRoot = yield* fileSystem.makeTempDirectoryScoped({
        directory: scriptsDirectory,
        prefix: ".stage-t3-runtime-test-",
      });
      const packageRoot = path.join(temporaryRoot, "source-package");
      yield* fileSystem.makeDirectory(packageRoot);
      yield* fileSystem.writeFileString(
        path.join(packageRoot, "package.json"),
        `${yield* encodeSourcePackage({ name: "source-package" })}\n`,
      );
      return yield* test(packageRoot);
    }),
  );

const assertPromiseRejects = (operation: () => Promise<unknown>, expectedMessage: RegExp) =>
  Effect.tryPromise(operation).pipe(
    Effect.match({
      onFailure: (error) => {
        assert.instanceOf(error.cause, Error);
        if (error.cause instanceof Error) assert.match(error.cause.message, expectedMessage);
      },
      onSuccess: () => assert.fail("Expected promise to reject."),
    }),
  );

const directoryLink = (target: string, link: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const platform = yield* HostProcessPlatform;
    yield* fileSystem.symlink(target, link);
    assert.equal(
      (yield* fileSystem.stat(link)).type,
      "Directory",
      platform === "win32" ? "Expected a directory junction." : "Expected a directory symlink.",
    );
  });

const runNode = (args: ReadonlyArray<string>, cwd?: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("node", args, {
          ...(cwd === undefined ? {} : { cwd }),
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        }),
      );
      const result = yield* Effect.all(
        {
          stdout: child.stdout.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (output, chunk) => output + chunk,
            ),
          ),
          stderr: child.stderr.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (output, chunk) => output + chunk,
            ),
          ),
          status: child.exitCode,
        },
        { concurrency: "unbounded" },
      );
      return { ...result, status: Number(result.status) };
    }),
  );

it.layer(NodeServices.layer)("stage-t3-runtime", (it) => {
  it.effect("is importable by plain Node without a TypeScript runtime loader", () =>
    Effect.gen(function* () {
      const moduleUrl = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(
        new URL("./stage-t3-runtime.mjs", import.meta.url).href,
      );
      const result = yield* runNode(["--input-type=module", "--eval", `import(${moduleUrl})`]);

      assert.equal(result.status, 0, result.stderr || result.stdout);
    }),
  );

  it.effect("pins the fork release package to the exact launcher version and provenance", () =>
    Effect.sync(() => {
      const packageJson = validateSourcePackage(serverPackageJson);

      assert.equal(packageJson.name, "t3");
      assert.equal(packageJson.version, RELEASE_VERSION);
      assert.equal(packageJson.repository.url, FORK_REPOSITORY_URL);
      assert.equal(packageJson.bin.t3, "./dist/bin.mjs");
      assert.include(packageJson.files, "dist");
    }),
  );

  it.effect("rejects non-canonical roots and anything inside a T3 home", () =>
    Effect.sync(() => {
      assert.throws(() => assertCanonicalStagingRoot("relative-stage", "/tmp/relative-stage"));
      assert.throws(() => assertCanonicalStagingRoot("/tmp/../stage", "/stage"));
      assert.throws(() => assertCanonicalStagingRoot("/tmp/stage", "/tmp/other-stage"));
      assert.throws(() =>
        assertCanonicalStagingRoot("/tmp/.t3/release-stage", "/tmp/.t3/release-stage"),
      );
      assert.throws(() => assertCanonicalStagingRoot("/", "/"));
      assert.equal(
        assertCanonicalStagingRoot("/tmp/t3-release-stage", "/tmp/t3-release-stage"),
        "/tmp/t3-release-stage",
      );
    }),
  );

  it.effect("falls back to a scoped direct node_modules package with no exports or main", () =>
    withTemporaryPackage((sourceRoot) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const dependencyName = "@scope/no-exports";
        const dependencyRoot = path.join(sourceRoot, "node_modules", "@scope", "no-exports");
        yield* fileSystem.makeDirectory(dependencyRoot, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(dependencyRoot, "package.json"),
          `${yield* encodePackageMetadata({ name: dependencyName, exports: {} })}\n`,
        );

        assert.equal(
          yield* Effect.tryPromise(() => resolvePackageRoot(sourceRoot, dependencyName)),
          yield* fileSystem.realPath(dependencyRoot),
        );
      }),
    ),
  );

  it.effect(
    "allows a direct package-manager symlink outside lexical node_modules when its identity matches",
    () =>
      withTemporaryPackage((sourceRoot) =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dependencyName = "@scope/no-exports";
          const packageStoreRoot = path.join(
            path.dirname(sourceRoot),
            "package-manager-store",
            "no-exports",
          );
          const dependencyLink = path.join(sourceRoot, "node_modules", "@scope", "no-exports");
          yield* fileSystem.makeDirectory(packageStoreRoot, { recursive: true });
          yield* fileSystem.writeFileString(
            path.join(packageStoreRoot, "package.json"),
            `${yield* encodePackageMetadata({ name: dependencyName, exports: {} })}\n`,
          );
          yield* fileSystem.makeDirectory(path.dirname(dependencyLink), { recursive: true });
          yield* directoryLink(packageStoreRoot, dependencyLink);

          assert.equal(
            yield* Effect.tryPromise(() => resolvePackageRoot(sourceRoot, dependencyName)),
            yield* fileSystem.realPath(packageStoreRoot),
          );
        }),
      ),
  );

  it.effect(
    "refuses unsafe lexical paths and direct package roots with wrong or missing metadata",
    () =>
      withTemporaryPackage((sourceRoot) =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          yield* assertPromiseRejects(
            () => resolvePackageRoot(sourceRoot, "../escape"),
            /Refusing unsafe dependency package name/,
          );
          yield* assertPromiseRejects(
            () => resolvePackageRoot(sourceRoot, "@scope/../escape"),
            /Refusing unsafe dependency package name/,
          );

          const packageStoreRoot = path.join(path.dirname(sourceRoot), "package-manager-store");
          const wrongNameLink = path.join(sourceRoot, "node_modules", "@scope", "wrong-name");
          const wrongNameRoot = path.join(packageStoreRoot, "wrong-name");
          yield* fileSystem.makeDirectory(wrongNameRoot, { recursive: true });
          yield* fileSystem.writeFileString(
            path.join(wrongNameRoot, "package.json"),
            `${yield* encodePackageMetadata({
              name: "@scope/not-the-requested-package",
              exports: {},
            })}\n`,
          );
          yield* fileSystem.makeDirectory(path.dirname(wrongNameLink), { recursive: true });
          yield* directoryLink(wrongNameRoot, wrongNameLink);
          yield* assertPromiseRejects(
            () => resolvePackageRoot(sourceRoot, "@scope/wrong-name"),
            /does not identify '@scope\/wrong-name'/,
          );

          const missingMetadataRoot = path.join(packageStoreRoot, "missing-metadata");
          const missingMetadataLink = path.join(
            sourceRoot,
            "node_modules",
            "@scope",
            "missing-metadata",
          );
          yield* fileSystem.makeDirectory(missingMetadataRoot, { recursive: true });
          yield* directoryLink(missingMetadataRoot, missingMetadataLink);
          yield* assertPromiseRejects(
            () => resolvePackageRoot(sourceRoot, "@scope/missing-metadata"),
            /missing valid package metadata/,
          );
        }),
      ),
  );

  it.effect(
    "stages a runnable runtime with package identity, sentinel, version, and preflight",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const temporaryRoot = yield* fileSystem.makeTempDirectoryScoped({
            directory: scriptsDirectory,
            prefix: ".stage-t3-runtime-test-",
          });
          const stagedByInvocationCwd = new Map<
            string,
            { versionRoot: string; entryPath: string; sentinelPath: string }
          >();
          for (const [index, cwd] of stageRuntimeInvocationCwds.entries()) {
            const stagingRoot = path.join(temporaryRoot, `staging-root-${String(index)}`);
            yield* fileSystem.makeDirectory(stagingRoot);
            const stage = yield* runNode(
              [stageRuntimeScriptPath, "--staging-root", stagingRoot],
              cwd,
            );

            assert.equal(stage.status, 0, stage.stderr || stage.stdout);
            stagedByInvocationCwd.set(cwd, yield* decodeStageResult(stage.stdout));
          }
          const staged = stagedByInvocationCwd.get(repositoryRoot);
          if (staged === undefined)
            return yield* Effect.die("Repository-root staging result is missing.");
          const stagingRoot = path.join(temporaryRoot, "staging-root-0");
          assert.equal(
            staged.versionRoot,
            path.join(stagingRoot, "runtime", "versions", RELEASE_VERSION),
          );
          assert.equal(
            staged.entryPath,
            path.join(staged.versionRoot, "node_modules", "t3", "dist", "bin.mjs"),
          );
          assert.equal(staged.sentinelPath, path.join(staged.versionRoot, ".install-complete"));
          assert.deepEqual(yield* fileSystem.readDirectory(stagingRoot), ["runtime"]);
          assert.isFalse(
            yield* fileSystem.exists(path.join(staged.versionRoot, ".staging-preflight.sqlite")),
          );

          const stagedT3Package = yield* decodePackageIdentity(
            yield* fileSystem.readFileString(
              path.join(staged.versionRoot, "node_modules", "t3", "package.json"),
            ),
          );
          assert.equal(stagedT3Package.name, "t3");
          assert.equal(stagedT3Package.version, RELEASE_VERSION);

          const externalPackageName = "@ff-labs/fff-node";
          const sourceExternalPackage = yield* decodePackageIdentity(
            yield* fileSystem.readFileString(
              path.join(
                repositoryRoot,
                "apps",
                "server",
                "node_modules",
                "@ff-labs",
                "fff-node",
                "package.json",
              ),
            ),
          );
          const stagedExternalPackage = yield* decodePackageIdentity(
            yield* fileSystem.readFileString(
              path.join(staged.versionRoot, "node_modules", "@ff-labs", "fff-node", "package.json"),
            ),
          );
          assert.equal(sourceExternalPackage.name, externalPackageName);
          assert.equal(stagedExternalPackage.name, externalPackageName);
          assert.equal(stagedExternalPackage.version, sourceExternalPackage.version);
          assert.equal(
            yield* fileSystem.readFileString(staged.sentinelPath),
            `${RELEASE_VERSION}\n`,
          );

          const version = yield* runNode([staged.entryPath, "--version"]);
          assert.equal(version.status, 0, version.stderr || version.stdout);
          assert.include(version.stdout, RELEASE_VERSION);

          const preflight = yield* runNode([
            staged.entryPath,
            "__service-preflight",
            "--database-path",
            path.join(temporaryRoot, "staged-preflight.sqlite"),
            "--launcher-protocol",
            String(SERVICE_LAUNCHER_PROTOCOL),
          ]);
          assert.equal(preflight.status, 0, preflight.stderr || preflight.stdout);
          const preflightResult = yield* decodePreflightResult(preflight.stdout);
          assert.equal(preflightResult.status, "ready");
          assert.equal(preflightResult.version, RELEASE_VERSION);
          assert.equal(preflightResult.launcherProtocol, SERVICE_LAUNCHER_PROTOCOL);
        }),
      ),
  );

  it.effect("returns failure without a success payload when the staging CLI cannot start", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const temporaryRoot = yield* fileSystem.makeTempDirectoryScoped({
          directory: scriptsDirectory,
          prefix: ".stage-t3-runtime-test-",
        });
        const result = yield* runNode(
          [stageRuntimeScriptPath, "--staging-root", path.join(temporaryRoot, "missing")],
          scriptsDirectory,
        );

        assert.notEqual(result.status, 0);
        assert.equal(result.stdout, "");
        assert.match(
          result.stderr,
          /\[stage-t3-runtime\] --staging-root must be an existing directory:/,
        );
      }),
    ),
  );
});
