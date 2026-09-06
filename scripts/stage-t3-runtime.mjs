#!/usr/bin/env node
/**
 * Assemble an already-built local t3 release into the exact immutable runtime
 * layout consumed by the Node service launcher. This deliberately stages only
 * below an explicit, canonical staging root; promotion and activation remain
 * launcher-owned operations.
 */
import * as ChildProcess from "node:child_process";
import { createRequire } from "node:module";
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  CLI_RUNTIME_EXTERNAL_PREFIXES,
  selectCliRuntimeExternalDependencies,
} from "./lib/cli-external-packages.mjs";
import { SERVICE_LAUNCHER_PROTOCOL } from "./lib/service-launcher-protocol.mjs";

export const RELEASE_VERSION = "0.0.40-fork.5";
export const FORK_REPOSITORY_URL = "https://github.com/jdrolls/t3code";
const RELEASE_ENTRY_PATH = Path.join("node_modules", "t3", "dist", "bin.mjs");
const SENTINEL_FILE = ".install-complete";
const PACKAGE_NAME_PATTERN = /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/i;

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function packageDependencyNames(packageJson) {
  return new Set([
    ...Object.keys(isRecord(packageJson.dependencies) ? packageJson.dependencies : {}),
    ...Object.keys(
      isRecord(packageJson.optionalDependencies) ? packageJson.optionalDependencies : {},
    ),
    ...Object.keys(isRecord(packageJson.peerDependencies) ? packageJson.peerDependencies : {}),
  ]);
}

function isOptionalDependency(packageJson, name) {
  return (
    (isRecord(packageJson.optionalDependencies) && name in packageJson.optionalDependencies) ||
    (isRecord(packageJson.peerDependenciesMeta) &&
      isRecord(packageJson.peerDependenciesMeta[name]) &&
      packageJson.peerDependenciesMeta[name].optional === true)
  );
}

export function validateSourcePackage(value) {
  if (!isRecord(value)) throw new Error("apps/server/package.json must be an object.");
  if (value.name !== "t3") throw new Error("The staged package must be named 't3'.");
  if (value.version !== RELEASE_VERSION) {
    throw new Error(`Expected t3 version ${RELEASE_VERSION}, found '${String(value.version)}'.`);
  }
  if (!isRecord(value.repository) || value.repository.url !== FORK_REPOSITORY_URL) {
    throw new Error(`Expected t3 repository ${FORK_REPOSITORY_URL}.`);
  }
  if (!isRecord(value.bin) || value.bin.t3 !== "./dist/bin.mjs") {
    throw new Error("The t3 package must expose ./dist/bin.mjs as its executable.");
  }
  if (!Array.isArray(value.files) || !value.files.includes("dist")) {
    throw new Error("The t3 package must publish its dist directory.");
  }
  return value;
}

/** Reject aliases, symlinks, filesystem roots, and conventional live T3 homes. */
export function assertCanonicalStagingRoot(rawPath, canonicalPath) {
  if (!Path.isAbsolute(rawPath) || rawPath !== Path.resolve(rawPath)) {
    throw new Error("--staging-root must be an absolute normalized path.");
  }
  if (canonicalPath !== rawPath) {
    throw new Error("--staging-root must be a canonical path, not a symlink or alias.");
  }
  if (Path.parse(canonicalPath).root === canonicalPath) {
    throw new Error("--staging-root cannot be a filesystem root.");
  }
  if (canonicalPath.split(Path.sep).includes(".t3")) {
    throw new Error("--staging-root must not be inside a T3 home.");
  }
  return canonicalPath;
}

async function canonicalStagingRoot(rawPath) {
  let canonicalPath;
  try {
    canonicalPath = await Fs.realpath(rawPath);
  } catch (cause) {
    throw new Error(`--staging-root must be an existing directory: ${String(cause)}`);
  }
  const stats = await Fs.stat(canonicalPath);
  if (!stats.isDirectory()) throw new Error("--staging-root must be a directory.");
  return assertCanonicalStagingRoot(rawPath, canonicalPath);
}

function assertSafePackageName(packageName) {
  if (typeof packageName !== "string" || !PACKAGE_NAME_PATTERN.test(packageName)) {
    throw new Error(`Refusing unsafe dependency package name '${String(packageName)}'.`);
  }
}

function isPathInside(parentPath, candidatePath) {
  const relative = Path.relative(parentPath, candidatePath);
  return (
    relative !== "" &&
    !relative.startsWith(`..${Path.sep}`) &&
    relative !== ".." &&
    !Path.isAbsolute(relative)
  );
}

function packageTarget(parentNodeModules, packageName) {
  assertSafePackageName(packageName);
  const target = Path.resolve(parentNodeModules, packageName);
  if (!isPathInside(Path.resolve(parentNodeModules), target)) {
    throw new Error(`Refusing dependency target outside staging root: '${packageName}'.`);
  }
  return target;
}

async function readPackageJson(packageRoot) {
  const packagePath = Path.join(packageRoot, "package.json");
  const contents = await Fs.readFile(packagePath, "utf8");
  try {
    return JSON.parse(contents);
  } catch (cause) {
    throw new Error(`Invalid package metadata at ${packagePath}: ${String(cause)}`);
  }
}

async function findPackageRoot(entryPath, expectedName) {
  let directory = Path.dirname(entryPath);
  for (;;) {
    try {
      const packageJson = await readPackageJson(directory);
      if (packageJson.name === expectedName) return await Fs.realpath(directory);
    } catch (cause) {
      if (!(cause instanceof Error) || !cause.message.startsWith("ENOENT")) throw cause;
    }
    const parent = Path.dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  throw new Error(`Could not locate package metadata for '${expectedName}'.`);
}

async function resolveDirectNodeModulesPackageRoot(fromPackageRoot, dependencyName) {
  const directNodeModules = Path.join(fromPackageRoot, "node_modules");
  // This check deliberately happens before realpath. Package managers commonly
  // make a direct dependency a symlink into their content-addressed store (or
  // another installed runtime), which is outside this lexical node_modules
  // directory after canonicalization. The requested name must nevertheless
  // select only its direct, lexical node_modules location.
  const candidateRoot = packageTarget(directNodeModules, dependencyName);
  const [candidateStats, canonicalPackageRoot] = await Promise.all([
    Fs.lstat(candidateRoot),
    Fs.realpath(candidateRoot),
  ]);
  if (!candidateStats.isDirectory() && !candidateStats.isSymbolicLink()) {
    throw new Error(
      `Direct node_modules package path for '${dependencyName}' is not a directory or symlink.`,
    );
  }

  const canonicalStats = await Fs.stat(canonicalPackageRoot);
  if (!canonicalStats.isDirectory()) {
    throw new Error(`Direct node_modules package root for '${dependencyName}' is not a directory.`);
  }

  let packageJson;
  try {
    packageJson = await readPackageJson(canonicalPackageRoot);
  } catch (cause) {
    throw new Error(
      `Direct node_modules package root for '${dependencyName}' is missing valid package metadata: ${String(cause)}`,
    );
  }
  if (!isRecord(packageJson) || packageJson.name !== dependencyName) {
    throw new Error(
      `Direct node_modules package metadata at ${canonicalPackageRoot} does not identify '${dependencyName}'.`,
    );
  }
  return canonicalPackageRoot;
}

export async function resolvePackageRoot(fromPackageRoot, dependencyName) {
  assertSafePackageName(dependencyName);
  const requireFromPackage = createRequire(Path.join(fromPackageRoot, "package.json"));
  try {
    return await Fs.realpath(
      Path.dirname(requireFromPackage.resolve(`${dependencyName}/package.json`)),
    );
  } catch (packageJsonCause) {
    try {
      return await findPackageRoot(requireFromPackage.resolve(dependencyName), dependencyName);
    } catch (entryPointCause) {
      try {
        // Some packages deliberately export neither their manifest nor a main
        // entry point. Their direct package directory is still a valid runtime
        // dependency, but only after its lexical target, canonical package
        // root, and manifest identity have all been validated.
        return await resolveDirectNodeModulesPackageRoot(fromPackageRoot, dependencyName);
      } catch (directPackageCause) {
        throw new Error(
          `Could not resolve package '${dependencyName}' from ${fromPackageRoot}; ` +
            `package metadata resolution failed: ${String(packageJsonCause)}; ` +
            `entry point resolution failed: ${String(entryPointCause)}; ` +
            `direct node_modules fallback failed: ${String(directPackageCause)}`,
        );
      }
    }
  }
}

async function copyPackageTree(input) {
  const { sourceRoot, targetRoot, ancestors } = input;
  await Fs.cp(sourceRoot, targetRoot, {
    recursive: true,
    dereference: true,
    errorOnExist: true,
    force: false,
    filter: (source) => {
      const relative = Path.relative(sourceRoot, source);
      return relative !== "node_modules" && !relative.startsWith(`node_modules${Path.sep}`);
    },
  });

  const packageJson = await readPackageJson(sourceRoot);
  const dependencies = [...packageDependencyNames(packageJson)].sort();
  for (const dependencyName of dependencies) {
    let dependencySourceRoot;
    try {
      dependencySourceRoot = await resolvePackageRoot(sourceRoot, dependencyName);
    } catch (cause) {
      if (isOptionalDependency(packageJson, dependencyName)) continue;
      throw new Error(
        `Could not resolve required runtime dependency '${dependencyName}' from ${sourceRoot}: ${String(cause)}`,
      );
    }
    // A cyclic dependency is already available at its ancestor destination.
    if (ancestors.has(dependencySourceRoot)) continue;
    const dependencyTargetRoot = packageTarget(
      Path.join(targetRoot, "node_modules"),
      dependencyName,
    );
    await Fs.mkdir(Path.dirname(dependencyTargetRoot), { recursive: true });
    await copyPackageTree({
      sourceRoot: dependencySourceRoot,
      targetRoot: dependencyTargetRoot,
      ancestors: new Set([...ancestors, dependencySourceRoot]),
    });
  }
}

function runNode(args) {
  return new Promise((resolve, reject) => {
    const child = ChildProcess.spawn("node", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", (cause) =>
      reject(new Error(`Could not run Node verification: ${cause.message}`)),
    );
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      reject(
        new Error(
          `Node verification failed (${signal ?? `exit ${String(code)}`}): ${stderr.trim() || stdout.trim()}`,
        ),
      );
    });
  });
}

async function verifyStagedRuntime(entryPath, preflightDatabasePath) {
  const version = await runNode([entryPath, "--version"]);
  if (!version.stdout.includes(RELEASE_VERSION)) {
    throw new Error(
      `Staged executable did not report ${RELEASE_VERSION}: ${version.stdout.trim()}`,
    );
  }
  const preflight = await runNode([
    entryPath,
    "__service-preflight",
    "--database-path",
    preflightDatabasePath,
    "--launcher-protocol",
    String(SERVICE_LAUNCHER_PROTOCOL),
  ]);
  let result;
  try {
    result = JSON.parse(preflight.stdout.trim());
  } catch (cause) {
    throw new Error(`Staged service preflight returned invalid JSON: ${String(cause)}`);
  }
  if (
    !isRecord(result) ||
    result.status !== "ready" ||
    result.version !== RELEASE_VERSION ||
    result.launcherProtocol !== SERVICE_LAUNCHER_PROTOCOL
  ) {
    throw new Error("Staged service preflight did not confirm the launcher contract.");
  }
}

export async function stageRuntime(rawStagingRoot) {
  const stagingRoot = await canonicalStagingRoot(rawStagingRoot);
  const repositoryRoot = await Fs.realpath(
    Path.resolve(Path.dirname(fileURLToPath(import.meta.url)), ".."),
  );
  const serverRoot = Path.join(repositoryRoot, "apps", "server");
  const sourcePackage = validateSourcePackage(await readPackageJson(serverRoot));
  const sourceDist = Path.join(serverRoot, "dist");
  const sourceEntry = Path.join(sourceDist, "bin.mjs");
  if (!(await Fs.stat(sourceEntry)).isFile()) {
    throw new Error(
      "Missing apps/server/dist/bin.mjs. Run `bun --cwd apps/server run build:bundle` first.",
    );
  }

  const versionRoot = Path.join(stagingRoot, "runtime", "versions", RELEASE_VERSION);
  try {
    await Fs.lstat(versionRoot);
    throw new Error(`Refusing to overwrite existing staged runtime: ${versionRoot}`);
  } catch (cause) {
    if (!(cause instanceof Error) || !cause.message.includes("ENOENT")) throw cause;
  }

  const stagePackageRoot = Path.join(versionRoot, "node_modules", "t3");
  try {
    await Fs.mkdir(stagePackageRoot, { recursive: true });
    await Fs.cp(sourceDist, Path.join(stagePackageRoot, "dist"), {
      recursive: true,
      dereference: true,
      errorOnExist: true,
      force: false,
    });
    await Fs.writeFile(
      Path.join(stagePackageRoot, "package.json"),
      `${JSON.stringify(sourcePackage, null, 2)}\n`,
      {
        mode: 0o600,
      },
    );

    const directRuntimeDependencies = selectCliRuntimeExternalDependencies(
      sourcePackage.dependencies,
    );
    for (const dependencyName of Object.keys(directRuntimeDependencies).sort()) {
      const dependencySourceRoot = await resolvePackageRoot(serverRoot, dependencyName);
      const dependencyTargetRoot = packageTarget(
        Path.join(versionRoot, "node_modules"),
        dependencyName,
      );
      await Fs.mkdir(Path.dirname(dependencyTargetRoot), { recursive: true });
      await copyPackageTree({
        sourceRoot: dependencySourceRoot,
        targetRoot: dependencyTargetRoot,
        ancestors: new Set([await Fs.realpath(serverRoot), dependencySourceRoot]),
      });
    }
    // Keep the runtime dependency policy load-bearing: an empty selection here
    // would make a superficially valid but non-runnable artifact.
    if (
      Object.keys(directRuntimeDependencies).length === 0 ||
      CLI_RUNTIME_EXTERNAL_PREFIXES.length === 0
    ) {
      throw new Error("No runtime external dependencies were selected for staging.");
    }

    const stagedEntry = Path.join(versionRoot, RELEASE_ENTRY_PATH);
    if (!(await Fs.stat(stagedEntry)).isFile()) {
      throw new Error(`Staged launcher entry is missing: ${stagedEntry}`);
    }
    // Preflight may create a SQLite database and sidecars. Keep all of those
    // outside the immutable version directory, then remove them even on failure.
    const preflightRoot = await Fs.mkdtemp(Path.join(stagingRoot, ".staging-preflight-"));
    try {
      await verifyStagedRuntime(stagedEntry, Path.join(preflightRoot, "database.sqlite"));
    } finally {
      await Fs.rm(preflightRoot, { recursive: true, force: true });
    }
    await Fs.writeFile(Path.join(versionRoot, SENTINEL_FILE), `${RELEASE_VERSION}\n`, {
      mode: 0o600,
    });
  } catch (cause) {
    await Fs.rm(versionRoot, { recursive: true, force: true });
    throw cause;
  }

  return {
    versionRoot,
    entryPath: Path.join(versionRoot, RELEASE_ENTRY_PATH),
    sentinelPath: Path.join(versionRoot, SENTINEL_FILE),
  };
}

function parseArgs(argv) {
  if (argv.length !== 2 || argv[0] !== "--staging-root") {
    throw new Error(
      "Usage: node scripts/stage-t3-runtime.mjs --staging-root <absolute-empty-directory>",
    );
  }
  return argv[1];
}

const entryPath = process.argv[1];
const isEntrypoint =
  entryPath !== undefined && import.meta.url === pathToFileURL(Path.resolve(entryPath)).href;
if (isEntrypoint) {
  stageRuntime(parseArgs(process.argv.slice(2)))
    .then((result) => process.stdout.write(`${JSON.stringify(result)}\n`))
    .catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      process.stderr.write(`[stage-t3-runtime] ${message}\n`);
      process.exitCode = 1;
    });
}
