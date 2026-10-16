import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";

// Use the parser locked with protoc-gen-es; the workspace's TS 7 CLI has no JS API.
const require = createRequire(import.meta.url);
let parser;
export const generationTypeScript = () => parser ??= createRequire(createRequire(require.resolve("@bufbuild/protoc-gen-es/package.json")).resolve("@bufbuild/protoplugin"))("typescript");

export const writeChanged = (path, content) => {
  const bytes = typeof content === "string" ? Buffer.from(content) : content;
  if (!existsSync(path) || !readFileSync(path).equals(bytes)) writeFileSync(path, bytes);
};

// Canonical generation inputs and their published copies live here.
// Every family that negotiates a protocol ships the shared handshake schema.
/** @type {[string, string[]][]} */
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["harness/v2/harness_pb", ["harness"]],
  ["protocol/v1/protocol_pb", ["filesystem", "harness"]],
  // inference_pb imports the proto2 custom options descriptor transitively.
  ["validation/v1/options_pb", ["inference", "actors", "filesystem", "machines", "objects", "stream"]],
  ["auth/v1/auth_pb", ["auth"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["objects/v1/objects_pb", ["objects"]],
  ["stream/v1/stream_pb", ["stream"]],
  ["stream/v1/session_pb", ["stream"]],
  ["actors/v1/actors_pb", ["actors"]],
];

export const compatibilityArtifacts = {
  auth: {
    schemaDigest: "proto/auth/v1/auth.proto",
    descriptorDigest: "rust/crates/auth/src/generated/acyclic-auth-v1.bin",
  },
  harness: {
    schemaDigest: "proto/harness/v2/harness.proto",
    conformanceDigest: "conformance/vectors/core.json",
  },
  filesystem: {
    schemaDigest: "proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v1/stream.proto",
    descriptorDigest: "rust/crates/stream/proto/stream/v1/stream_descriptor.bin",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
    conformanceDigest: "conformance/vectors/objects-v1.json",
  },
  machines: {
    schemaDigest: "proto/machines/v1/machines.proto",
    descriptorDigest: "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
    conformanceDigest: "conformance/vectors/machines.json",
  },
  inference: {
    schemaDigest: "proto/inference/v1/inference.proto",
    descriptorDigest: "rust/crates/inference/inference_descriptor.bin",
    conformanceDigest: "conformance/vectors/inference.json",
  },
  actors: {
    schemaDigest: "proto/actors/v1/actors.proto",
    descriptorDigest: "compatibility/descriptors/actors-v1.bin",
  },
};

export const generatedDescriptors = [
  ["proto/auth", compatibilityArtifacts.auth.descriptorDigest],
  ["proto/filesystem", compatibilityArtifacts.filesystem.descriptorDigest],
  ["proto/objects/v1", "rust/crates/objects/src/generated/acyclic-objects-v1.bin"],
  ["proto/machines", compatibilityArtifacts.machines.descriptorDigest],
  ["proto/inference", compatibilityArtifacts.inference.descriptorDigest],
  ["proto/actors", compatibilityArtifacts.actors.descriptorDigest],
  ["rust/crates/stream/proto/stream", compatibilityArtifacts.stream.descriptorDigest],
];

export const nativeWasmVector = "conformance/vectors/harness/native-wasm-event-v2.json";
export const packagedSourceCopies = [
  ["proto/validation/v1/options.proto", "rust/crates/contract-derive/proto/validation/v1/options.proto"],
  ["proto/validation/v1/options.proto", "rust/crates/stream/proto/validation/v1/options.proto"],
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant", "execution-placement", "task-admission", "workflow-admission",
    "interaction-resolution", "resolution-receipt", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  [compatibilityArtifacts.harness.schemaDigest, "rust/crates/harness/proto/harness/v2/harness.proto"],
  ["proto/protocol/v1/protocol.proto", "rust/crates/harness/proto/protocol/v1/protocol.proto"],
  [compatibilityArtifacts.harness.conformanceDigest, "rust/crates/conformance/vectors/harness.json"],
  [nativeWasmVector, "rust/crates/harness/conformance/native-wasm-event-v2.json"],
  ["conformance/vectors/stream.json", "rust/crates/stream/conformance/stream.json"],
  ["conformance/vectors/stream.json", "rust/crates/conformance/vectors/stream.json"],
  ["conformance/vectors/objects-v1.json", "rust/crates/objects/conformance/objects-v1.json"],
  ["conformance/vectors/objects-v1.json", "rust/crates/conformance/vectors/objects-v1.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  [compatibilityArtifacts.filesystem.conformanceDigest, "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
];

// Rust splitting and feature gates are emitted by sdk-proto-codegen.
export const protoCodegenCommand = args => process.env.SDK_PROTO_CODEGEN
  ? [process.env.SDK_PROTO_CODEGEN, ...args]
  : ["cargo", "run", "--offline", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", ...args];

export const projectGeneratedRust = (root, directory) => {
  const [command, ...args] = protoCodegenCommand(["rust-projection", directory]);
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Rust projection failed: ${result.stderr}`);
};

export const normalizeGeneratedRust = (_relative, source) => `${source.trimEnd()}\n`;

export const packagedRustDirectories = [
  ["acyclic/objects/v1", "rust/crates/objects/src/generated"],
  ["acyclic/machines/v1", "rust/crates/machines/src/generated"],
];

export const rustBindingFiles = directory => {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      for (const child of rustBindingFiles(`${directory}/${entry.name}`)) files.push(`${entry.name}/${child}`);
    } else if (entry.name.endsWith(".rs")) files.push(entry.name);
  }
  return files.sort();
};

export const normalizeGeneratedTypeScript = source => {
  const normalized = `${source.trimEnd()}\n`;
  if (normalized.split("\n").length < 800) return normalized;
  // Reflow only lexer trivia: every generated type and runtime token survives.
  const ts = generationTypeScript();
  const syntax = ts.createSourceFile("generated.ts", normalized, ts.ScriptTarget.Latest, true);
  const literals = new Map();
  const collect = node => {
    if (ts.isLiteralExpression(node) || ts.isTemplateExpression(node)) {
      literals.set(node.getStart(syntax), node.end);
    } else ts.forEachChild(node, collect);
  };
  collect(syntax);
  const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, normalized);
  const parts = [];
  let column = 0;
  let lineComment = false;
  for (let kind = scanner.scan(); kind !== ts.SyntaxKind.EndOfFileToken; kind = scanner.scan()) {
    let text = scanner.getTokenText();
    const end = literals.get(scanner.getTokenPos());
    if (end !== undefined) {
      text = normalized.slice(scanner.getTokenPos(), end);
      scanner.setTextPos(end);
    } else if (kind === ts.SyntaxKind.NewLineTrivia) {
      text = lineComment || column >= 180 ? "\n" : " ";
      lineComment = false;
    } else if (kind === ts.SyntaxKind.WhitespaceTrivia) {
      text = " ";
    } else if (kind === ts.SyntaxKind.MultiLineCommentTrivia && text.startsWith("/**")) {
      text = `/** ${text.slice(3, -2).split("\n").map(line => line.trim().replace(/^\* ?/, "")).filter(Boolean).join(" ")} */`;
    } else if (kind === ts.SyntaxKind.SingleLineCommentTrivia) {
      lineComment = true;
    }
    if (text === "\n") while (parts.at(-1) === " ") parts.pop();
    parts.push(text);
    column = text.includes("\n") ? text.length - text.lastIndexOf("\n") - 1 : column + text.length;
  }
  const result = `${parts.join("").trimEnd()}\n`;
  if (result.split("\n").length >= 800) throw new Error("generated TypeScript exceeds the source-file bound");
  const projected = ts.createSourceFile("generated.ts", result, ts.ScriptTarget.Latest, true);
  const printer = ts.createPrinter({ removeComments: true });
  if (syntax.parseDiagnostics.length || projected.parseDiagnostics.length || printer.printFile(syntax) !== printer.printFile(projected)) {
    throw new Error("generated TypeScript reflow changed syntax");
  }
  return result;
};
