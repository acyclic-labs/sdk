import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename, dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const forbiddenContent = [
  /acyclic(?:[-_.:/]|\\)(?:internal|private)(?:[-_.:/\\]|$)/i,
  /(?:package|import)\s+["']?[^\s"']*(?:internal|private)[^\s"']*/i,
];
const privateKey = /BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY/;
const credentialLiteral = /\b(?:aws_secret_access_key|github_token|authorization)["']?\s*[=:]\s*(["'`])((?:\\.|(?!\1)[^\r\n])*)\1/gi;
const credentialAssignment = /\b(?:aws_secret_access_key|github_token|authorization:(?!:))\s*[=:]\s*[^\s${][^\s]*/i;
// Only the recorded public fake-server fixtures may contain credential literals.
const fixtureDigests = new Map([
  ["typescript/packages/machines/test/fixtures/server-key.pem", "048bbcff062c2c0ccb0ed19df146e59b9cc937f86210be0158076123f24b2340"],
  ["typescript/packages/swarm/test/http.test.mjs", "daa0e6ba7f5f0ec32786a32ebcb5f7abc2903e8b24927c5de9a6c2b44a03f418"],
  ["typescript/packages/stream/test/stream.test.ts", "7dac2ff0fb1055805b058c0237ccb752851f3e680e620f29227f93a681b6b3f2"],
  ["typescript/packages/inference/test/client.test.ts", "a3ef1f09996032eeb1afd4d6862fef537a61103e479bf0b17234e9a4f89d4f69"],
  ["typescript/packages/inference/test/idle-kv.test.ts", "6ee7f3e2aaeb15ea681f5de19cd3be5ddfb5a9c9b699d0cd40178ee6e55def58"],
]);
const forbiddenPath = /(?:^|[\\/])(?:proto|rust|typescript)[\\/](?:.*[\\/])?(?:internal|private)(?:[\\/]|$)/i;
const machinesPath = /(?:^|[\\/])(?:proto[\\/]machines|rust[\\/]crates[\\/]machines|typescript[\\/]packages[\\/]machines|generated[\\/](?:rust|typescript)[\\/](?:acyclic[\\/])?machines)(?:[\\/]|$)/i;
const forbiddenMachinesContent = /\b(?:vmm|fleet|scheduler|daemon|placement)\b|host_profile|guest_control|qualification_release/i;
const failures = [];

function hasCredentialLiteral(content) {
  credentialLiteral.lastIndex = 0;
  let match;
  while ((match = credentialLiteral.exec(content)) !== null) {
    credentialLiteral.lastIndex = match.index + 1;
    const [, delimiter, value] = match;
    if (delimiter !== "`" || !/(?<!\\)(?:\\\\)*\$\{/.test(value)) return true;
  }
  return false;
}

function inspect(path, content) {
  const relativePath = relative(root, path).replaceAll("\\", "/");
  if (forbiddenPath.test(relativePath)) failures.push(`${relativePath}: forbidden private path`);
  for (const pattern of forbiddenContent) if (pattern.test(content)) failures.push(`${relativePath}: ${pattern}`);
  const hasCredential = /\.(?:[cm]?[jt]sx?)$/.test(relativePath)
    ? hasCredentialLiteral(content)
    : credentialAssignment.test(content);
  if (privateKey.test(content) || hasCredential) {
    const digest = createHash("sha256").update(content.replaceAll("\r\n", "\n")).digest("hex");
    if (fixtureDigests.get(relativePath) !== digest) failures.push(`${relativePath}: credential outside a recorded fake-server fixture`);
  }
  if (machinesPath.test(relativePath) && forbiddenMachinesContent.test(content)) failures.push(`${relativePath}: forbidden managed-service implementation domain`);
}

for (const [path, content] of [
  ["proto/private/admin.proto", "syntax = \"proto3\";"],
  ["example.rs", "use acyclic_" + "internal::scheduler;"],
  ["credential.txt", "-----BEGIN " + "PRIVATE KEY-----"],
  ["rust/crates/machines/proto/machines/v1/leak.proto", "package vmm.fleet.v1;"],
]) {
  const before = failures.length;
  inspect(join(root, path), content);
  if (failures.length === before) throw new Error(`boundary self-test did not reject ${basename(path)}`);
}
failures.length = 0;
const paths = execFileSync("git", ["ls-files", "-z"], { cwd: root, encoding: "utf8" }).split("\u0000").filter(Boolean);
for (const path of paths) {
  try {
    const content = await readFile(join(root, path), "utf8");
    if (!content.includes("\u0000")) inspect(join(root, path), content);
  } catch (error) {
    failures.push(`${path}: unreadable (${error.message})`);
  }
}
if (failures.length) { console.error(failures.join("\n")); process.exitCode = 1; }
