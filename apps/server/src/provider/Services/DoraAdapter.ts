/** DoraAdapter — per-instance Dora JSONL runtime contract. */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface DoraAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
