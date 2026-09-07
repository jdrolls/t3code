export declare const RELEASE_VERSION: "0.0.40-fork.5";
export declare const FORK_REPOSITORY_URL: "https://github.com/jdrolls/t3code";

export interface SourcePackage {
  readonly name: string;
  readonly version: string;
  readonly repository: {
    readonly url: string;
  };
  readonly bin: {
    readonly t3: string;
  };
  readonly files: ReadonlyArray<string>;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly peerDependenciesMeta?: Readonly<Record<string, { readonly optional?: boolean }>>;
}

export interface StagedRuntime {
  readonly versionRoot: string;
  readonly entryPath: string;
  readonly sentinelPath: string;
}

export declare function validateSourcePackage(value: unknown): SourcePackage;
export declare function assertCanonicalStagingRoot(rawPath: string, canonicalPath: string): string;
export declare function resolvePackageRoot(
  fromPackageRoot: string,
  dependencyName: string,
): Promise<string>;
export declare function stageRuntime(rawStagingRoot: string): Promise<StagedRuntime>;
