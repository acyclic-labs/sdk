/** The actor client uses the shared bounded Rust session and platform pool. */
export * from "./client-query.js";
export { prepareAgentInvocation } from "./client-command.js";
export { initializeClientConnection, resolveConnectionOptions, type ClientConnectionError, type ClientConnectionOptions, type ClientEnvironment } from "./client-connection.js";
export { verifyClientControls, verifyClientToolOutput, verifyClientObservation, type ClientControlFixture, type ClientControlVector,
  type ClientObservationFixture, type ClientToolOutputFixture } from "./client-conformance.js";
