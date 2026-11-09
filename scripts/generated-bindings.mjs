import { execFileSync } from "node:child_process";
import ignore from "ignore";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = fileURLToPath(new URL("../", import.meta.url));
let tracked;

// Pinned outputs are compared. Ignored build outputs are regenerated from the
// current source, so a clean checkout needs no pre-existing generated cache.
export function projectGenerated(path, content, check) {
  if (check) {
    tracked ??= new Set(execFileSync("git", ["ls-files", "-z"], { cwd: sourceRoot, encoding: "utf8" }).split("\0"));
    if (tracked.has(relative(sourceRoot, path).replaceAll("\\", "/"))) return checkGenerated(path, content);
  }
  writeChanged(path, content);
}

export function checkGenerated(path, content) {
  if (!existsSync(path) || !readFileSync(path).equals(Buffer.from(content))) {
    throw new Error(`STEP_FAILED generated-artifact: ${path} is stale; recovery: bun run generate`);
  }
}

// Exporters render privately in both modes; only changed bytes reach the checkout.
export async function generateFiles(destination, render, prune = [], check = false) {
  const temporary = mkdtempSync(join(tmpdir(), "sdk-generation-"));
  try {
    await render(temporary);
    const current = new Set();
    for (const entry of readdirSync(temporary, { recursive: true, withFileTypes: true })) {
      if (!entry.isFile()) continue;
      const source = join(entry.parentPath, entry.name);
      const name = relative(temporary, source);
      current.add(name);
      projectGenerated(join(destination, name), readFileSync(source), check);
    }
    for (const directory of prune.map(name => join(destination, name))) {
      if (!existsSync(directory)) continue;
      for (const entry of readdirSync(directory, { recursive: true, withFileTypes: true })) {
        const path = join(entry.parentPath, entry.name);
        if (entry.isFile() && !current.has(relative(destination, path))) {
          if (check) throw new Error(`STEP_FAILED generated-artifact: obsolete ${path}; recovery: bun run generate`);
          rmSync(path);
        }
      }
    }
  } finally { rmSync(temporary, { recursive: true, force: true }); }
}

export const writeChanged = (path, content) => {
  const bytes = typeof content === "string" ? Buffer.from(content) : content;
  if (!existsSync(path) || !readFileSync(path).equals(bytes)) {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, bytes);
  }
};

// Canonical generation inputs and their published copies live here.
// Every family that negotiates a protocol ships the shared handshake schema.
/** @type {[string, string[], string?][]} */
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["protocol/v1/protocol_pb", ["filesystem"]],
  // Service bindings import the proto2 custom options descriptor transitively.
  ["validation/v1/options_pb", ["auth", "inference", "actors", "filesystem", "machines", "objects", "stream"], "compatibility"],
  ["auth/v1/auth_pb", ["auth"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["harness/v2/resources_pb", ["machines", "actors"], "compatibility"],
  ["objects/v1/objects_pb", ["objects"]],
  ["stream/v1/stream_pb", ["stream"]],
  ["stream/v1/session_pb", ["stream"]],
  ["actors/v1/actors_pb", ["actors"]],
];

// These packages intentionally own no protobuf outputs after protocol retirement.
export const retiredTypeScriptBindingPackages = ["harness"];

export const compatibilityArtifacts = {
  auth: {
    schemaDigest: "rust/crates/auth/proto/auth/v1/auth.proto",
    descriptorDigest: "compatibility/descriptors/auth-v1.bin",
    conformanceDigest: "rust/crates/auth/conformance/secrets-v1.json",
  },
  harness: {
    schemaDigest: "rust/crates/harness-core/proto/harness/v2/harness.proto",
    conformanceDigest: "rust/crates/harness/conformance/legacy-harness.json",
  },
  filesystem: {
    schemaDigest: "rust/crates/filesystem-wire/proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "compatibility/descriptors/filesystem-v2.bin",
    conformanceDigest: "rust/crates/filesystem/conformance/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v1/stream.proto",
    descriptorDigest: "compatibility/descriptors/stream-v1.bin",
    conformanceDigest: "rust/crates/stream/conformance/stream.json",
  },
  objects: {
    schemaDigest: "rust/crates/objects/proto/objects/v1/objects.proto",
    descriptorDigest: "compatibility/descriptors/objects-v1.bin",
    conformanceDigest: "rust/crates/objects/conformance/objects-v1.json",
  },
  machines: {
    schemaDigest: "rust/crates/machines/proto/machines/v1/machines.proto",
    descriptorDigest: "compatibility/descriptors/machines-v1.bin",
    conformanceDigest: "rust/crates/machines/conformance/machines.json",
  },
  inference: {
    schemaDigest: "rust/crates/inference/proto/inference/v1/inference.proto",
    descriptorDigest: "compatibility/descriptors/inference-v1.bin",
    conformanceDigest: "rust/crates/inference/conformance/inference.json",
  },
  actors: {
    schemaDigest: "rust/crates/actors/proto/actors/v1/actors.proto",
    descriptorDigest: "compatibility/descriptors/actors-v1.bin",
  },
};

export const nativeWasmVector = "rust/crates/harness/conformance/native-wasm-event-v2.json";
// Only checkout-owned copies belong here. Validate at import so every consumer,
// including generators, rejects build outputs before reading or writing them.
export const packagedSourceCopies = [
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant",
    "interaction-resolution", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  ["rust/crates/harness-core/proto/protocol/v1/protocol.proto", "rust/crates/filesystem-wire/proto/protocol/v1/protocol.proto"],
];

// Evaluate source-owned policy even in exported trees. Parent repositories,
// global excludes and the presence of a Git index cannot change this inventory.
const ignoreRules = new Map();
function ignoredSourceCopy(copy) {
  const parts = copy.split("/");
  const rules = [];
  for (let depth = 0; depth < parts.length; depth++) {
    const directory = parts.slice(0, depth).join("/");
    if (!ignoreRules.has(directory)) {
      const path = join(sourceRoot, directory, ".gitignore");
      ignoreRules.set(directory, ignore({ ignorecase: false }).add(existsSync(path) ? readFileSync(path, "utf8") : ""));
    }
    rules.push(ignoreRules.get(directory));
    // Stop at an excluded parent: a child's negation cannot re-include it.
    for (let index = depth; index >= 0; index--) {
      const path = parts.slice(index, depth + 1).join("/") + (depth < parts.length - 1 ? "/" : "");
      const result = rules[index].test(path);
      if (result.ignored) return true;
      if (result.unignored) break;
    }
  }
  return false;
}
const ignoredCopies = packagedSourceCopies.map(([, copy]) => copy).filter(ignoredSourceCopy);
if (ignoredCopies.length > 0) {
  throw new Error(`STEP_FAILED packaged-source-copy-ignored: ${ignoredCopies.join(", ")}; remove ignored copies from packagedSourceCopies`);
}
