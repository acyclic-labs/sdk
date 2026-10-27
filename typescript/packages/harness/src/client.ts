/** The actor client uses the shared bounded Rust session and platform pool. */
export * from "./client-query.js";
export { verifyClientControls, verifyClientToolOutput, type ClientControlFixture, type ClientControlVector,
  type ClientToolOutputFixture } from "./client-conformance.js";
