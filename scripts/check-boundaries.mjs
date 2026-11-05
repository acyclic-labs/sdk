import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
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
  ["typescript/packages/inference/test/client.test.ts", "69bf9379c89e4412c81c0228dc996a2eb624fb94f253e601bddacb492300452b"],
  ["typescript/packages/inference/test/idle-kv.test.ts", "6ee7f3e2aaeb15ea681f5de19cd3be5ddfb5a9c9b699d0cd40178ee6e55def58"],
]);
export const credentialFixturePaths = [...fixtureDigests.keys()];
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
  if (relativePath.endsWith(".rs") && /\b(?:option_)?env\s*!\s*\(\s*"CARGO_BIN_EXE_/.test(content)) {
    failures.push(`${relativePath}: executable lookup: use acyclic_test_support::cargo_binary!; verify with node scripts/check-boundaries.mjs`);
  }
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

async function main() {
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
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
