import { describe, expect, test } from "bun:test";
import {
  defaultNativeProcessOwner,
  retryNativeProcessTermination,
  spawnNativeProcess,
  terminateNativeProcess,
} from "../src/native-process-node.js";

describe("Node process ownership host adapter", () => {
  test("exports one shared owner surface for GraphCoder and native hosts", () => {
    expect(defaultNativeProcessOwner.spawn).toBe(spawnNativeProcess);
    expect(defaultNativeProcessOwner.terminate).toBe(terminateNativeProcess);
    expect(typeof retryNativeProcessTermination).toBe("function");
  });
});
