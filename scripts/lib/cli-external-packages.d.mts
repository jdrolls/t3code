export declare const CLI_RUNTIME_EXTERNAL_PREFIXES: readonly string[];
export declare const CLI_BUILD_ONLY_EXTERNAL_PREFIXES: readonly string[];
export declare const CLI_EXTERNAL_PACKAGE_PREFIXES: readonly string[];

export declare function isRuntimeExternalCliDependency(id: string): boolean;
export declare function isExternalCliDependency(id: string): boolean;
export declare function shouldBundleCliDependency(id: string): boolean;
export declare function selectCliRuntimeExternalDependencies(
  dependencies: Readonly<Record<string, string>>,
): Record<string, string>;
export declare function findInlinedExternalPackages(source: string): {
  readonly regionCount: number;
  readonly inlined: ReadonlyArray<string>;
  readonly inlinedPackages: ReadonlyArray<string>;
};
