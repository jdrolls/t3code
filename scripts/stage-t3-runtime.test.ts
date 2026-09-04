import * as ChildProcess from "node:child_process";
import * as Fs from "node:fs/promises";
import * as Path from "node:path";

import { assert, it } from "@effect/vitest";

import serverPackageJson from "../apps/server/package.json" with { type: "json" };
import {
  assertCanonicalStagingRoot,
  FORK_REPOSITORY_URL,
  RELEASE_VERSION,
  resolvePackageRoot,
  validateSourcePackage,
} from "./stage-t3-runtime.mjs";
import { SERVICE_LAUNCHER_PROTOCOL } from "./lib/service-launcher-protocol.mjs";

async function withTemporaryPackage(test: (packageRoot: string) => Promise<void>) {
  const temporaryRoot = await Fs.mkdtemp(Path.join(process.cwd(), ".stage-t3-runtime-test-"));
  try {
    const packageRoot = Path.join(temporaryRoot, "source-package");
    await Fs.mkdir(packageRoot);
    await Fs.writeFile(Path.join(packageRoot, "package.json"), '{"name":"source-package"}\n');
    await test(packageRoot);
  } finally {
    await Fs.rm(temporaryRoot, { recursive: true, force: true });
  }
}

async function assertPromiseRejects(promise: Promise<unknown>, expectedMessage: RegExp): Promise<void> {
  await promise.then(
    () => assert.fail("Expected promise to reject."),
    (cause: unknown) => {
      assert.instanceOf(cause, Error);
      if (cause instanceof Error) assert.match(cause.message, expectedMessage);
    },
  );
}

it("is importable by plain Node without a TypeScript runtime loader", () => {
  const moduleUrl = new URL("./stage-t3-runtime.mjs", import.meta.url).href;
  const result = ChildProcess.spawnSync(
    "node",
    ["--input-type=module", "--eval", `import(${JSON.stringify(moduleUrl)})`],
    { encoding: "utf8" },
  );

  assert.strictEqual(result.status, 0, result.stderr || result.stdout);
});

it("pins the fork release package to the exact launcher version and provenance", () => {
  const packageJson = validateSourcePackage(serverPackageJson);

  assert.equal(packageJson.name, "t3");
  assert.equal(packageJson.version, RELEASE_VERSION);
  assert.equal(packageJson.repository.url, FORK_REPOSITORY_URL);
  assert.equal(packageJson.bin.t3, "./dist/bin.mjs");
  assert.include(packageJson.files, "dist");
});

it("rejects non-canonical roots and anything inside a T3 home", () => {
  assert.throws(() => assertCanonicalStagingRoot("relative-stage", "/tmp/relative-stage"));
  assert.throws(() => assertCanonicalStagingRoot("/tmp/../stage", "/stage"));
  assert.throws(() => assertCanonicalStagingRoot("/tmp/stage", "/tmp/other-stage"));
  assert.throws(() => assertCanonicalStagingRoot("/tmp/.t3/release-stage", "/tmp/.t3/release-stage"));
  assert.throws(() => assertCanonicalStagingRoot("/", "/"));
  assert.equal(assertCanonicalStagingRoot("/tmp/t3-release-stage", "/tmp/t3-release-stage"), "/tmp/t3-release-stage");
});

it("falls back to a scoped direct node_modules package with no exports or main", async () => {
  await withTemporaryPackage(async (sourceRoot) => {
    const dependencyName = "@scope/no-exports";
    const dependencyRoot = Path.join(sourceRoot, "node_modules", "@scope", "no-exports");
    await Fs.mkdir(dependencyRoot, { recursive: true });
    await Fs.writeFile(
      Path.join(dependencyRoot, "package.json"),
      `${JSON.stringify({ name: dependencyName, exports: {} })}\n`,
    );

    assert.equal(await resolvePackageRoot(sourceRoot, dependencyName), await Fs.realpath(dependencyRoot));
  });
});

it("allows a direct package-manager symlink outside lexical node_modules when its identity matches", async () => {
  await withTemporaryPackage(async (sourceRoot) => {
    const dependencyName = "@scope/no-exports";
    const packageStoreRoot = Path.join(Path.dirname(sourceRoot), "package-manager-store", "no-exports");
    const dependencyLink = Path.join(sourceRoot, "node_modules", "@scope", "no-exports");
    await Fs.mkdir(packageStoreRoot, { recursive: true });
    await Fs.writeFile(
      Path.join(packageStoreRoot, "package.json"),
      `${JSON.stringify({ name: dependencyName, exports: {} })}\n`,
    );
    await Fs.mkdir(Path.dirname(dependencyLink), { recursive: true });
    await Fs.symlink(packageStoreRoot, dependencyLink, process.platform === "win32" ? "junction" : "dir");

    assert.equal(await resolvePackageRoot(sourceRoot, dependencyName), await Fs.realpath(packageStoreRoot));
  });
});

it("refuses unsafe lexical paths and direct package roots with wrong or missing metadata", async () => {
  await withTemporaryPackage(async (sourceRoot) => {
    await assertPromiseRejects(
      resolvePackageRoot(sourceRoot, "../escape"),
      /Refusing unsafe dependency package name/,
    );
    await assertPromiseRejects(
      resolvePackageRoot(sourceRoot, "@scope/../escape"),
      /Refusing unsafe dependency package name/,
    );

    const packageStoreRoot = Path.join(Path.dirname(sourceRoot), "package-manager-store");
    const wrongNameLink = Path.join(sourceRoot, "node_modules", "@scope", "wrong-name");
    const wrongNameRoot = Path.join(packageStoreRoot, "wrong-name");
    await Fs.mkdir(wrongNameRoot, { recursive: true });
    await Fs.writeFile(
      Path.join(wrongNameRoot, "package.json"),
      `${JSON.stringify({ name: "@scope/not-the-requested-package", exports: {} })}\n`,
    );
    await Fs.mkdir(Path.dirname(wrongNameLink), { recursive: true });
    await Fs.symlink(wrongNameRoot, wrongNameLink, process.platform === "win32" ? "junction" : "dir");
    await assertPromiseRejects(
      resolvePackageRoot(sourceRoot, "@scope/wrong-name"),
      /does not identify '@scope\/wrong-name'/,
    );

    const missingMetadataRoot = Path.join(packageStoreRoot, "missing-metadata");
    const missingMetadataLink = Path.join(sourceRoot, "node_modules", "@scope", "missing-metadata");
    await Fs.mkdir(missingMetadataRoot, { recursive: true });
    await Fs.symlink(
      missingMetadataRoot,
      missingMetadataLink,
      process.platform === "win32" ? "junction" : "dir",
    );
    await assertPromiseRejects(
      resolvePackageRoot(sourceRoot, "@scope/missing-metadata"),
      /missing valid package metadata/,
    );
  });
});

it("stages a runnable runtime with package identity, sentinel, version, and preflight", async () => {
  const temporaryRoot = await Fs.mkdtemp(Path.join(process.cwd(), ".stage-t3-runtime-test-"));
  try {
    const stagingRoot = Path.join(temporaryRoot, "staging-root");
    await Fs.mkdir(stagingRoot);
    const scriptPath = Path.resolve("scripts/stage-t3-runtime.mjs");
    const stage = ChildProcess.spawnSync("node", [scriptPath, "--staging-root", stagingRoot], {
      encoding: "utf8",
    });

    assert.equal(stage.error, undefined, stage.error?.message);
    assert.equal(stage.status, 0, stage.stderr || stage.stdout);
    const staged = JSON.parse(stage.stdout) as {
      versionRoot: string;
      entryPath: string;
      sentinelPath: string;
    };
    assert.equal(staged.versionRoot, Path.join(stagingRoot, "runtime", "versions", RELEASE_VERSION));
    assert.equal(staged.entryPath, Path.join(staged.versionRoot, "node_modules", "t3", "dist", "bin.mjs"));
    assert.equal(staged.sentinelPath, Path.join(staged.versionRoot, ".install-complete"));
    assert.deepEqual(await Fs.readdir(stagingRoot), ["runtime"]);
    await assertPromiseRejects(
      Fs.access(Path.join(staged.versionRoot, ".staging-preflight.sqlite")),
      /ENOENT/,
    );

    const stagedT3Package = JSON.parse(
      await Fs.readFile(Path.join(staged.versionRoot, "node_modules", "t3", "package.json"), "utf8"),
    ) as { name: unknown; version: unknown };
    assert.equal(stagedT3Package.name, "t3");
    assert.equal(stagedT3Package.version, RELEASE_VERSION);

    const externalPackageName = "@ff-labs/fff-node";
    const sourceExternalPackage = JSON.parse(
      await Fs.readFile(Path.join("apps", "server", "node_modules", "@ff-labs", "fff-node", "package.json"), "utf8"),
    ) as { name: unknown; version: unknown };
    const stagedExternalPackage = JSON.parse(
      await Fs.readFile(
        Path.join(staged.versionRoot, "node_modules", "@ff-labs", "fff-node", "package.json"),
        "utf8",
      ),
    ) as { name: unknown; version: unknown };
    assert.equal(sourceExternalPackage.name, externalPackageName);
    assert.equal(stagedExternalPackage.name, externalPackageName);
    assert.equal(stagedExternalPackage.version, sourceExternalPackage.version);
    assert.equal(await Fs.readFile(staged.sentinelPath, "utf8"), `${RELEASE_VERSION}\n`);

    const version = ChildProcess.spawnSync("node", [staged.entryPath, "--version"], { encoding: "utf8" });
    assert.equal(version.status, 0, version.stderr || version.stdout);
    assert.include(version.stdout, RELEASE_VERSION);

    const preflight = ChildProcess.spawnSync(
      "node",
      [
        staged.entryPath,
        "__service-preflight",
        "--database-path",
        Path.join(temporaryRoot, "staged-preflight.sqlite"),
        "--launcher-protocol",
        String(SERVICE_LAUNCHER_PROTOCOL),
      ],
      { encoding: "utf8" },
    );
    assert.equal(preflight.status, 0, preflight.stderr || preflight.stdout);
    const preflightResult = JSON.parse(preflight.stdout) as Record<string, unknown>;
    assert.equal(preflightResult.status, "ready");
    assert.equal(preflightResult.version, RELEASE_VERSION);
    assert.equal(preflightResult.launcherProtocol, SERVICE_LAUNCHER_PROTOCOL);
  } finally {
    await Fs.rm(temporaryRoot, { recursive: true, force: true });
  }
});

it("returns failure without a success payload when the staging CLI cannot start", () => {
  const scriptPath = Path.resolve("scripts/stage-t3-runtime.mjs");
  const missingRoot = Path.join(process.cwd(), `.stage-t3-runtime-missing-${process.pid}`);
  const result = ChildProcess.spawnSync("node", [scriptPath, "--staging-root", missingRoot], { encoding: "utf8" });

  assert.notEqual(result.status, 0);
  assert.equal(result.stdout, "");
  assert.match(result.stderr, /\[stage-t3-runtime\] --staging-root must be an existing directory:/);
});
