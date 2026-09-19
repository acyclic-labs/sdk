import { describe, expect, test } from "bun:test";
import {
  encodeGitCompatCommand,
  finishGitCompatOutput,
  gitCompatSafeTimestamp,
  parseGitCompatOutputJson,
  parseGitPendingTransitionJson,
  stringifyGitFilesystemResult,
  type GitCompatCommand,
  type GitCompatOutput,
  type GitFilesystemAction,
  type GitFilesystemResult,
} from "../src/compat.js";

const workspace = Uint8Array.from({ length: 16 }, (_, index) => index);
const generation = Uint8Array.from({ length: 32 }, (_, index) => index);
const commit = "ab".repeat(32);

describe("Git compatibility command codec", () => {
  test("encodes every advertised typed command", () => {
    const commands: readonly GitCompatCommand[] = [
      { kind: "status" },
      { kind: "diff", cached: true },
      { kind: "log", maximum: 20 },
      { kind: "show", object: "HEAD" },
      { kind: "add", paths: ["."] },
      { kind: "commit", message: "message", author: "agent", authoredAtSeconds: 10n },
      { kind: "branch", create: "topic" },
      { kind: "switch", branch: "topic", create: true },
      { kind: "restore", source: "HEAD", paths: ["file"] },
      { kind: "reset", target: "HEAD", mode: "hard" },
      { kind: "merge", branch: "topic" },
      { kind: "rebase", branch: "main" },
      { kind: "stash-push" },
      { kind: "stash-pop" },
      { kind: "cherry-pick", object: commit },
      { kind: "revert", object: commit },
      { kind: "tag", name: "v1", target: commit },
      { kind: "blame", path: "file" },
      { kind: "grep", pattern: "needle", path: "src" },
      { kind: "clean", dryRun: true },
      { kind: "archive", object: "HEAD" },
      { kind: "apply", patch: new TextEncoder().encode("patch") },
      { kind: "bisect", arguments: ["start", "bad", "good"] },
    ];
    expect(commands.map((command) => JSON.stringify(encodeGitCompatCommand(command)))).toHaveLength(23);
    expect(encodeGitCompatCommand(commands[9]!)).toEqual({
      Reset: { target: "HEAD", mode: "Hard" },
    });
    expect(() => encodeGitCompatCommand({
      kind: "commit",
      message: "overflow",
      author: "agent",
      authoredAtSeconds: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
    })).toThrow(RangeError);
    expect(gitCompatSafeTimestamp(10n)).toBe(10);
    expect(() => gitCompatSafeTimestamp(BigInt(Number.MAX_SAFE_INTEGER) + 1n)).toThrow(RangeError);
  });

  test("normalizes identities, optionals, and snake-case status output", () => {
    const parsed = parseGitCompatOutputJson(JSON.stringify({
      Status: {
        branch: "main",
        head: null,
        workspace: Array.from(generation),
        dirty: true,
        all_changes_staged: true,
      },
    }));
    expect(parsed).toEqual({ Status: {
      branch: "main",
      head: undefined,
      workspace: generation,
      dirty: true,
      allChangesStaged: true,
    } });
  });

  test("round-trips prepared actions and executor results as byte arrays", () => {
    const action: GitFilesystemAction = { RestoreGeneration: {
      workspace_id: workspace,
      generation,
      paths: undefined,
    } };
    const pending = parseGitPendingTransitionJson(JSON.stringify({
      id: Array.from(workspace),
      action: { RestoreGeneration: {
        workspace_id: Array.from(workspace),
        generation: Array.from(generation),
        paths: null,
      } },
      mutation: "NoOp",
    }));
    expect(pending.id).toEqual(workspace);
    expect(pending.action).toEqual(action);

    const result: GitFilesystemResult = { Captured: {
      generation,
      workspace_id: workspace,
      tracked_paths: ["file"],
    } };
    expect(JSON.parse(stringifyGitFilesystemResult(result))).toEqual({ Captured: {
      generation: Array.from(generation),
      workspace_id: Array.from(workspace),
      tracked_paths: ["file"],
    } });
    expect(JSON.parse(stringifyGitFilesystemResult({ Data: {
      kind: "nested-bytes",
      value: { bytes: new Uint8Array([1, 2]), nested: [new Uint8Array([3])] },
    } }))).toEqual({ Data: {
      kind: "nested-bytes",
      value: { bytes: [1, 2], nested: [[3]] },
    } });
  });

  test("rejects malformed machine-readable output", () => {
    expect(() => parseGitCompatOutputJson('{"Status":{"branch":"main"}}')).toThrow(TypeError);
    expect(() => parseGitCompatOutputJson(JSON.stringify({
      Prepared: {
        transition: [1, 2],
        action: { ApplyPatch: { patch: [256] } },
      },
    }))).toThrow(TypeError);
    expect(() => parseGitCompatOutputJson('{"Unknown":{}}')).toThrow(TypeError);
  });
});

describe("Git compatibility durable execution", () => {
  test("returns the filesystem result when a no-op transition completes", async () => {
    const action: GitFilesystemAction = { ApplyPatch: { patch: [1, 2, 3] } };
    const result: GitFilesystemResult = { Applied: { generation } };
    let completed: Uint8Array | undefined;
    const output = await finishGitCompatOutput(
      {
        async completeTransitionResult(transition, actual) {
          completed = transition;
          expect(actual).toEqual(result);
          return "NoOp";
        },
      },
      { Prepared: { transition: workspace, action } },
      { async execute(operationId, actual) {
        expect(operationId).toEqual(workspace);
        expect(actual).toEqual(action);
        return result;
      } },
    );
    expect(completed).toEqual(workspace);
    expect(output).toEqual({ Filesystem: result });
  });

  test("rejects a non-durable action", async () => {
    const output: GitCompatOutput = { Action: { ApplyPatch: { patch: [1] } } };
    await expect(finishGitCompatOutput(
      { async completeTransitionResult() { return "NoOp"; } },
      output,
      { async execute() { return { Applied: { generation: undefined } }; } },
    )).rejects.toThrow("without a durable transition");
  });
});
