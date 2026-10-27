import {
  AgentControls, PreparedAgentCommand, encodeCanonicalJson,
  type AgentCommand, type PreparedAgentInvocation,
} from "./client-query.js";

/** Prepare one typed command on an existing authenticated session. Rust owns
 * canonical encoding, schema admission and the exact invocation bytes. The
 * caller retains the returned invocation across execution errors and frees it
 * only after choosing how to settle or preserve that outcome. */
export function prepareAgentInvocation(controls: AgentControls, value: AgentCommand): PreparedAgentInvocation {
  const command = new PreparedAgentCommand(encodeCanonicalJson(value));
  try {
    return controls.prepare(command);
  } finally {
    command.free();
  }
}
