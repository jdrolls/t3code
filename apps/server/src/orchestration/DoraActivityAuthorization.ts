import type { ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";

/**
 * Server-only proof that the WebSocket transport authenticated a Dora
 * control-plane client. It is object-identity based, never serializable, and
 * is accepted only when minted by this module.
 */
export interface AuthenticatedDoraActivityCapability {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
}

const issuedCapabilities = new WeakSet<object>();

/**
 * Mint this only after checking the dedicated Dora control-plane scope. The
 * identities remain explicit in the wire command and are checked again by the
 * engine against the current session read model.
 */
export function createAuthenticatedDoraActivityCapability(
  input: AuthenticatedDoraActivityCapability,
): AuthenticatedDoraActivityCapability {
  const capability = Object.freeze({ ...input });
  issuedCapabilities.add(capability);
  return capability;
}

export function isAuthenticatedDoraActivityCapability(
  value: unknown,
): value is AuthenticatedDoraActivityCapability {
  return typeof value === "object" && value !== null && issuedCapabilities.has(value);
}
