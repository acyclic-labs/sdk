// Ordinary configured hook process. Record stdin and its chosen response.
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
const [record, action] = process.argv.slice(2);
const input = JSON.parse(readFileSync(0, "utf8"));
let response;
if (input.hook_event_name === "PreToolUse" && ["ask", "allow", "deny"].includes(action)) {
  response = { hookSpecificOutput: {
    permissionDecision: action, permissionDecisionReason: "E2E approval policy",
  } };
} else if (action === "context") response = { hookSpecificOutput: { additionalContext: "HOOK_E2E_CONTEXT" } };
appendFileSync(record, JSON.stringify({ ...input, fixture_action: action ?? null, fixture_response: response ?? null }) + "\n");
// This is a protocol producer: write the exact response to its inherited fd,
// without console's lazy socket initialization or buffered logging.
if (response) writeFileSync(1, JSON.stringify(response) + "\n");
if (input.hook_event_name === "PreToolUse" && action === "fail") {
  writeFileSync(2, "E2E_HOOK_FAILURE\n");
  process.exitCode = 1;
}
