import { readFile, readdir } from "node:fs/promises";
import { basename, dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const ignored = new Set([".git", "node_modules", "target", "dist"]);
const forbiddenCodeContent = [
  /acyclic(?:[-_.:/]|\\)(?:internal|private)(?:[-_.:/\\]|$)/i,
  /(?:package|import)\s+["']?[^\s"']*(?:internal|private)[^\s"']*/i,
];
const forbiddenCredentialContent = [
  /BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY/,
  /(?:aws_secret_access_key|github_token|authorization:(?!:))\s*[=:]\s*[^\s${][^\s]*/i,
];
const forbiddenPath = /(?:^|[\\/])(?:proto|rust|typescript)[\\/](?:.*[\\/])?(?:internal|private)(?:[\\/]|$)/i;
const machinesPath = /(?:^|[\\/])(?:proto[\\/]machines|rust[\\/]crates[\\/]machines|typescript[\\/]packages[\\/]machines|generated[\\/](?:rust|typescript)[\\/](?:acyclic[\\/])?machines)(?:[\\/]|$)/i;
const forbiddenMachinesContent = /\b(?:vmm|fleet|scheduler|daemon|placement)\b|host_profile|guest_control|qualification_release/i;
const fixtureCredentialFiles = new Set([
  "scripts/graphcoder-platform-gates.provenance.test.mjs",
  "scripts/graphcoder-production-pty.test.mjs",
]);
// These tests intentionally pass a harmless credential-shaped key through the
// environment filter. Keep the exception tied to those files and values;
// production source and arbitrary test credentials remain rejected.
const fixtureCredentialLiteral = /\bAWS_SECRET_ACCESS_KEY\s*:\s*"(?:discard|must-not-pass)"/giu;
const failures = [];

function stripComments(content) {
  // Boundary terms in prose are not dependencies or credentials. Preserve
  // quoted source while removing comments without relying on a third-party
  // parser in this standalone gate.
  let output = "";
  let quote = null;
  for (let index = 0; index < content.length; index += 1) {
    const current = content[index];
    const next = content[index + 1];
    if (quote !== null) {
      output += current;
      if (current === "\\") {
        if (next !== undefined) output += next;
        index += 1;
      } else if (current === quote) {
        quote = null;
      }
      continue;
    }
    if (current === "\"" || current === "'" || current === "`") {
      quote = current;
      output += current;
      continue;
    }
    if (current === "/" && next === "/") {
      index += 2;
      while (index < content.length && content[index] !== "\n") index += 1;
      if (index < content.length) output += "\n";
      continue;
    }
    if (current === "/" && next === "*") {
      index += 2;
      while (index < content.length && !(content[index] === "*" && content[index + 1] === "/")) {
        if (content[index] === "\n") output += "\n";
        index += 1;
      }
      if (index < content.length) index += 1;
      continue;
    }
    output += current;
  }
  return output;
}

function normalizedPath(path) {
  return path.replaceAll("\\", "/");
}

function contentForCredentialScan(relativePath, content) {
  if (!fixtureCredentialFiles.has(normalizedPath(relativePath))) return content;
  return content.replace(fixtureCredentialLiteral, "fixture_credential: \"redacted\"");
}

function inspect(path, content) {
  const relativePath = relative(root, path);
  if (forbiddenPath.test(relativePath)) failures.push(`${relativePath}: forbidden private path`);
  if (normalizedPath(relativePath) === "scripts/check-boundaries.mjs") return;
  const code = stripComments(content);
  for (const pattern of forbiddenCodeContent) if (pattern.test(code)) failures.push(`${relativePath}: ${pattern}`);
  const credentialContent = contentForCredentialScan(relativePath, code);
  for (const pattern of forbiddenCredentialContent) if (pattern.test(credentialContent)) failures.push(`${relativePath}: ${pattern}`);
  if (machinesPath.test(relativePath) && forbiddenMachinesContent.test(code)) failures.push(`${relativePath}: forbidden managed-service implementation domain`);
}

async function visit(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (ignored.has(entry.name)) continue;
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await visit(path);
    else {
      try {
        const content = await readFile(path, "utf8");
        if (!content.includes("\u0000")) inspect(path, content);
      } catch (error) {
        failures.push(`${relative(root, path)}: unreadable (${error.message})`);
      }
    }
  }
}

for (const [path, content] of [
  ["proto/private/admin.proto", "syntax = \"proto3\";"],
  ["example.rs", "use acyclic_" + "internal::scheduler;"],
  ["example.mjs", "import \"@acyclic-labs/private-runtime\";"],
  ["credential.env", "AWS_SECRET_ACCESS_KEY = live-secret-value"],
  ["credential.txt", "-----BEGIN " + "PRIVATE KEY-----"],
  ["proto/machines/v1/leak.proto", "package vmm.fleet.v1;"],
]) {
  const before = failures.length;
  inspect(join(root, path), content);
  if (failures.length === before) throw new Error(`boundary self-test did not reject ${basename(path)}`);
}
for (const [path, content] of [
  ["comment.js", "// import private/package\n/* AWS_SECRET_ACCESS_KEY = redacted */\n"],
  ["scripts/graphcoder-platform-gates.provenance.test.mjs", "const fixture = { AWS_SECRET_ACCESS_KEY: \"discard\" };"],
]) {
  const before = failures.length;
  inspect(join(root, path), content);
  if (failures.length !== before) throw new Error(`boundary self-test falsely rejected ${basename(path)}`);
}
failures.length = 0;
await visit(root);
if (failures.length) { console.error(failures.join("\n")); process.exitCode = 1; }
