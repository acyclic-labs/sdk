import { describe, expect, test } from "bun:test";
import { once } from "node:events";
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

  test("does not inherit ambient credentials when no environment is supplied", async () => {
    const child = spawnNativeProcess(process.execPath, ["-e", "process.stdout.write(process.env.GRAPHCODER_HOST_SECRET ?? 'missing')"], {
      stdio: ["ignore", "pipe", "ignore"],
    });
    let output = "";
    child.stdout?.on("data", chunk => { output += String(chunk); });
    await once(child, "close");
    expect(output).toBe("missing");
  });
});
