import assert from "node:assert/strict";
import { parseContractJson } from "./contract-targets.mjs";

// The very same JSON boundary admits Cargo metadata, artifact records and
// exporter bundles. Seed with captured output; malformed bytes must preserve
// their actual parser cause rather than becoming an unrelated refusal.
export function fuzz(data) {
  try {
    parseContractJson(Buffer.from(data).toString("utf8"), "contract-payload: malformed bundle from byte input");
    return true;
  } catch (error) {
    assert.ok(error.cause instanceof SyntaxError);
    assert.match(error.message, /^STEP_FAILED contract-payload:.*recovery:/);
    return false;
  }
}
