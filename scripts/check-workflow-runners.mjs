import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const directory = ".github/workflows";
const githubHosted = [];
const permissionScopes = new Set([
  "actions",
  "attestations",
  "checks",
  "contents",
  "deployments",
  "discussions",
  "id-token",
  "issues",
  "models",
  "packages",
  "pages",
  "pull-requests",
  "repository-projects",
  "security-events",
  "statuses",
]);
for (const name of readdirSync(directory).filter(name => /\.ya?ml$/.test(name)).sort()) {
  const path = join(directory, name);
  const source = readFileSync(path, "utf8");
  for (const line of source.split(/\r?\n/)) {
    const match = line.match(/^\s*(?:runner|runs-on):\s*(["']?)(ubuntu|windows|macos)-([^\s#"']+)\1\s*(?:#.*)?$/);
    if (match) githubHosted.push(`${path}:${match[2]}-${match[3]}`);
  }
  if (source.includes("self-hosted")) throw new Error(`${path} uses a self-hosted runner`);
  if (/^\s*shell:\s*\$\{\{/m.test(source)) {
    throw new Error(`${path} computes a shell dynamically; use the runner default or a literal shell`);
  }
  for (const match of source.matchAll(/^\s{2,}(?<scope>[a-z][a-z-]+):\s*(?:read|write|none)\s*$/gm)) {
    const scope = match.groups.scope;
    if (!permissionScopes.has(scope)) {
      throw new Error(`${path} uses unknown permission scope ${scope}`);
    }
  }
}

const expected = [
  ".github\\workflows\\publish-crate.yml:ubuntu-24.04",
  ".github\\workflows\\publish-npm.yml:ubuntu-24.04",
];
if (process.platform !== "win32") {
  for (let index = 0; index < expected.length; index += 1) {
    expected[index] = expected[index].replaceAll("\\", "/");
  }
}
if (JSON.stringify(githubHosted) !== JSON.stringify(expected)) {
  throw new Error(`GitHub-hosted runner boundary changed: ${JSON.stringify(githubHosted)}`);
}
