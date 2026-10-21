// Ordinary configured hook process. Record the exact stdin envelope.
import { appendFileSync, readFileSync } from "node:fs";
const [record, decision] = process.argv.slice(2);
const input = JSON.parse(readFileSync(0, "utf8"));
appendFileSync(record, JSON.stringify(input) + "\n");
if (input.hook_event_name === "PreToolUse" && decision === "ask") {
  console.log(JSON.stringify({ hookSpecificOutput: {
    permissionDecision: "ask", permissionDecisionReason: "E2E approval policy",
  } }));
} else console.log(JSON.stringify({ hookSpecificOutput: { additionalContext: "HOOK_E2E_CONTEXT" } }));
