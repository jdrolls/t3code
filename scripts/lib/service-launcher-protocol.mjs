/**
 * Node-loadable contract shared by release tooling and the bundled service.
 * Protocol 2 snapshots SQLite before trials so migrations can be rolled back safely.
 * Bump this only when the launcher and staged runtime protocol changes together.
 */
export const SERVICE_LAUNCHER_PROTOCOL = 2;
