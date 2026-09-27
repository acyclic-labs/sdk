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
const workspaceUuid = "00010203-0405-0607-0809-0a0b0c0d0e0f";
const generation = Uint8Array.from({ length: 32 }, (_, index) => index);
const commit = "ab".repeat(32);
const exactTree = { kind: "exact" as const, workspace_id: workspace, generation };
const exactTreeJson = { kind: "exact", workspace_id: Array.from(workspace), generation: Array.from(generation) };

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
        workspace: exactTreeJson,
        dirty: "dirty",
        all_changes_staged: true,
      },
    }));
    expect(parsed).toEqual({ Status: {
      branch: "main",
      head: undefined,
      workspace: exactTree,
      dirty: "dirty",
      allChangesStaged: true,
    } });
  });

  test("round-trips prepared actions and executor results as byte arrays", () => {
    const action: GitFilesystemAction = { RestoreGeneration: {
      tree: exactTree,
      paths: undefined,
    } };
    const pending = parseGitPendingTransitionJson(JSON.stringify({
      id: workspaceUuid,
      action: { RestoreGeneration: {
        tree: exactTreeJson,
        paths: null,
      } },
      mutation: "NoOp",
    }));
    expect(pending.id).toEqual(workspace);
    expect(pending.action).toEqual(action);

    const result: GitFilesystemResult = { Captured: {
      tree: exactTree,
      tracked_paths: ["file"],
      proof: undefined,
    } };
    expect(JSON.parse(stringifyGitFilesystemResult(result))).toEqual({ Captured: {
      tree: exactTreeJson,
      tracked_paths: ["file"],
      proof: null,
    } });
    expect(JSON.parse(stringifyGitFilesystemResult({ Data: {
      kind: "nested-bytes",
      value: { bytes: new Uint8Array([1, 2]), nested: [new Uint8Array([3])] },
    } }))).toEqual({ Data: {
      kind: "nested-bytes",
      value: { bytes: [1, 2], nested: [[3]] },
    } });
  });

  test("preserves lazy trees, tracked paths, and issued capture proofs", () => {
    const lazyTreeJson = {
      kind: "lazy", id: Array.from(generation), workspace_id: Array.from(workspace),
      authored_generation: Array.from(generation),
      source: { identity: Array.from(workspace), epoch: 7 },
      overlay: Array.from(generation), shadows: Array.from(generation),
    };
    const prepared = parseGitCompatOutputJson(JSON.stringify({ Prepared: {
      transition: workspaceUuid,
      action: { CaptureCommit: {
        workspace_tree: lazyTreeJson, head_tree: exactTreeJson,
        head_workspace_tree: exactTreeJson, tracked_paths: ["kept.txt"],
        message: "commit", author: "agent", authored_at_seconds: 10, expected_head: commit,
      } },
    } }));
    expect(prepared).toEqual({ Prepared: {
      transition: workspace,
      action: { CaptureCommit: {
        workspace_tree: {
          kind: "lazy", id: generation, workspace_id: workspace,
          authored_generation: generation, source: { identity: workspace, epoch: 7n },
          overlay: generation, shadows: generation,
        },
        head_tree: exactTree, head_workspace_tree: exactTree,
        tracked_paths: ["kept.txt"], message: "commit", author: "agent",
        authored_at_seconds: 10, expected_head: commit,
      } },
    } });
    const proof = {
      fork_parent: exactTree, initial_generation: generation, operation_id: workspace,
    };
    const captured: GitFilesystemResult = { Captured: {
      tree: exactTree, tracked_paths: ["kept.txt"], proof,
    } };
    const wire = JSON.parse(stringifyGitFilesystemResult(captured));
    expect(wire).toEqual({ Captured: {
      tree: exactTreeJson, tracked_paths: ["kept.txt"],
      proof: {
        fork_parent: exactTreeJson, initial_generation: Array.from(generation),
        operation_id: workspaceUuid,
      },
    } });
    expect(parseGitCompatOutputJson(JSON.stringify({ Filesystem: wire }))).toEqual({ Filesystem: captured });
  });

  test("preserves a full u64 lazy source epoch", () => {
    const epoch = 18_446_744_073_709_551_615n;
    const lazy = {
      kind: "lazy" as const, id: generation, workspace_id: workspace,
      authored_generation: generation,
      source: { identity: workspace, epoch }, overlay: generation, shadows: generation,
    };
    const result: GitFilesystemResult = { Applied: { tree: lazy, tracked_paths: undefined } };
    const wire = stringifyGitFilesystemResult(result);
    expect(wire).toContain('"epoch":18446744073709551615');
    expect(parseGitCompatOutputJson(`{"Filesystem":${wire}}`)).toEqual({ Filesystem: result });
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
    const result: GitFilesystemResult = { Applied: { tree: exactTree, tracked_paths: undefined } };
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
      { async execute() { return { Applied: { tree: undefined, tracked_paths: undefined } }; } },
    )).rejects.toThrow("without a durable transition");
  });
});
