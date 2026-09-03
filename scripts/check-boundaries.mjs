import { readFile, readdir } from "node:fs/promises";
import { basename, dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const ignored = new Set([".git", "node_modules", "target", "dist"]);
const forbiddenContent = [
  /acyclic(?:[-_.:/]|\\)(?:internal|private)(?:[-_.:/\\]|$)/i,
  /(?:package|import)\s+["']?[^\s"']*(?:internal|private)[^\s"']*/i,
  /BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY/,
  /(?:aws_secret_access_key|github_token|authorization:)\s*[=:]\s*[^\s${][^\s]*/i,
];
const forbiddenPath = /(?:^|[\\/])(?:proto|rust|typescript)[\\/](?:.*[\\/])?(?:internal|private)(?:[\\/]|$)/i;
const machinesPath = /(?:^|[\\/])(?:proto[\\/]machines|rust[\\/]crates[\\/]machines|typescript[\\/]packages[\\/]machines|generated[\\/](?:rust|typescript|openapi)[\\/](?:acyclic[\\/])?machines)(?:[\\/]|$)/i;
const forbiddenMachinesContent = /\b(?:vmm|fleet|scheduler|daemon|placement)\b|host_profile|guest_control|qualification_release/i;
const failures = [];

function inspect(path, content) {
  const relativePath = relative(root, path);
  if (forbiddenPath.test(relativePath)) failures.push(`${relativePath}: forbidden private path`);
  for (const pattern of forbiddenContent) if (pattern.test(content)) failures.push(`${relativePath}: ${pattern}`);
  if (machinesPath.test(relativePath) && forbiddenMachinesContent.test(content)) failures.push(`${relativePath}: forbidden managed-service implementation domain`);
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
  ["credential.txt", "-----BEGIN " + "PRIVATE KEY-----"],
  ["proto/machines/v1/leak.proto", "package vmm.fleet.v1;"],
]) {
  const before = failures.length;
  inspect(join(root, path), content);
  if (failures.length === before) throw new Error(`boundary self-test did not reject ${basename(path)}`);
}
failures.length = 0;
await visit(root);
if (failures.length) { console.error(failures.join("\n")); process.exitCode = 1; }
