import { expect, test } from "bun:test";
import { messageViewExample } from "../examples/client-message.js";

test("public TypeScript message example uses actual reducer admission and original retry identity", async () => {
  const trace = await messageViewExample();
  expect(trace.provisional).toBe("11:1");
  expect(trace.unknownOutcome).toBe("Unknown");
  expect(trace.finalProvenance).toBeNull();
  expect(trace.finalPrediction).toBe("Confirmed");
  expect(trace.canonicalMessages).toBe(1n);
});
