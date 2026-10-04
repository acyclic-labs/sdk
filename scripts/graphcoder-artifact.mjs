#!/usr/bin/env node

// Emit the exact artifact record consumed by the qualification receipt. This
// records bytes and source provenance at the packaging boundary; it does not
// build or publish an artifact.

import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { parse, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { assertCanonicalParents } from "./graphcoder-path-ownership.mjs";
import { workingTreeDigest } from "./graphcoder-source-fence.mjs";

const HEX40 = /^[0-9a-f]{40}$/;

function fail(message) {
  throw new Error(`graphcoder-artifact: ${message}`);
}

function git(cwd, ...args) {
  return execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8" }).trim();
}

function regularFile(path) {
  assertCanonicalParents(parse(path).root, path, "artifact", false);
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata || !metadata.isFile() || metadata.isSymbolicLink()) fail(`artifact must be an existing regular file: ${path}`);
}

function timestamp(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) fail("built_at must be an ISO timestamp");
  return parsed.toISOString();
}

function producerReceipt({ value, artifactPath, sha256, sourceCommit, sourceTree, buildId, built_at, receiptBase, required = false, requireCausal = false, workingTreeDigestFn = workingTreeDigest }) {
  let receipt = value;
  if (typeof receipt === "string") {
    try { receipt = JSON.parse(readFileSync(resolve(receiptBase, receipt), "utf8")); }
    catch (error) { fail(`producer receipt is unreadable: ${error instanceof Error ? error.message : String(error)}`); }
  }
  if (receipt === undefined && required) fail(`producer receipt is required for generated artifact: ${artifactPath}`);
  if (receipt === undefined) {
    // Source and driver files are direct observations. Generated outputs must
    // provide the producer receipt emitted by their build boundary.
    receipt = {
      protocol: "acyclic.graphcoder.producer-receipt.v1",
      producer_id: buildId,
      artifact_path: artifactPath,
      sha256,
      source_commit: sourceCommit,
      source_tree: sourceTree,
      observed_at: built_at,
      observed_after_dispatch: false,
      exit_code: null,
      origin: "direct-observation",
    };
  }
  if (Array.isArray(receipt.outputs) && receipt.artifact_path === undefined) {
    const output = receipt.outputs.find(item => item && resolve(receiptBase, item.path) === artifactPath);
    if (!output) fail(`producer receipt does not list artifact: ${artifactPath}`);
    receipt = { ...receipt, artifact_path: artifactPath, sha256: output.sha256 };
  }
  if (!receipt || receipt.protocol !== "acyclic.graphcoder.producer-receipt.v1") fail("producer receipt protocol is invalid");
  if (receipt.artifact_path !== artifactPath || receipt.sha256 !== sha256 || receipt.source_commit !== sourceCommit || receipt.source_tree !== sourceTree) {
    fail("producer receipt does not match artifact bytes or source");
  }
  if (typeof receipt.producer_id !== "string" || receipt.producer_id.trim() === "") fail("producer receipt lacks producer_id");
  if (typeof receipt.observed_after_dispatch !== "boolean") fail("producer receipt lacks observed_after_dispatch");
  if (requireCausal && receipt.observed_after_dispatch !== true) fail(`producer receipt does not prove a post-dispatch artifact: ${artifactPath}`);
  if (receipt.observed_after_dispatch === true) {
    if (typeof receipt.attempt_nonce !== "string" || receipt.attempt_nonce.trim() === "") fail("producer receipt lacks attempt_nonce");
    if (typeof receipt.invocation_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(receipt.invocation_sha256)) fail("producer receipt lacks invocation_sha256");
    if (typeof receipt.source_working_tree_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(receipt.source_working_tree_sha256)) fail("producer receipt lacks source_working_tree_sha256");
    if (receipt.source_working_tree_sha256 !== workingTreeDigestFn(receiptBase)) fail("producer receipt source working-tree digest does not match the current checkout");
  }
  return receipt;
}

export function describeArtifact({ path, sourceCwd = process.cwd(), buildId, builtAt, fresh = true, producerReceipt: suppliedReceipt, requiredProducerReceipt = false, requireCausalReceipt = false, workingTreeDigestFn = workingTreeDigest, gitOps = { git } }) {
  if (typeof path !== "string" || path.trim() === "") fail("path must be nonempty");
  const cwd = resolve(sourceCwd);
  const artifactPath = resolve(cwd, path);
  if (!existsSync(artifactPath)) fail(`artifact does not exist: ${artifactPath}`);
  regularFile(artifactPath);
  if (typeof buildId !== "string" || buildId.trim() === "") fail("build_id must be nonempty");
  if (typeof fresh !== "boolean") fail("fresh must be boolean");
  const sourceCommit = gitOps.git(cwd, "rev-parse", "HEAD");
  const sourceTree = gitOps.git(cwd, "rev-parse", "HEAD^{tree}");
  if (!/^[0-9a-f]{40}$/.test(sourceCommit) || !HEX40.test(sourceTree)) fail("source repository returned invalid commit/tree");
  const built_at = timestamp(builtAt ?? new Date().toISOString());
  const sha256 = createHash("sha256").update(readFileSync(artifactPath)).digest("hex");
  const producer_receipt = producerReceipt({ value: suppliedReceipt, artifactPath, sourceCommit, sourceTree, sha256, buildId: buildId.trim(), built_at, receiptBase: cwd, required: requiredProducerReceipt, requireCausal: requireCausalReceipt, workingTreeDigestFn });
  return {
    path: artifactPath,
    sha256,
    source_commit: sourceCommit,
    source_tree: sourceTree,
    built_at,
    build_id: buildId.trim(),
    fresh: producer_receipt.observed_after_dispatch,
    producer_receipt,
  };
}

function usage() {
  console.error("usage: graphcoder-artifact.mjs describe PATH BUILD_ID [BUILT_AT] [SOURCE_CWD]");
  process.exitCode = 2;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [, , command, path, buildId, builtAt, sourceCwd] = process.argv;
    if (command !== "describe" || path === undefined || buildId === undefined || process.argv.length > 7) usage();
    else process.stdout.write(`${JSON.stringify(describeArtifact({ path, buildId, builtAt, sourceCwd }), null, 2)}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
