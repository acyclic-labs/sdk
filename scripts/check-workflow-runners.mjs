import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const directory = ".github/workflows";
const githubHosted = [];
for (const name of readdirSync(directory).filter(name => /\.ya?ml$/.test(name)).sort()) {
  const path = join(directory, name);
  const source = readFileSync(path, "utf8");
  for (const line of source.split(/\r?\n/)) {
    const match = line.match(/^\s*(?:runner|runs-on):\s*(["']?)(ubuntu|windows|macos)-([^\s#"']+)\1\s*(?:#.*)?$/);
    if (match) githubHosted.push(`${path}:${match[2]}-${match[3]}`);
  }
  if (source.includes("self-hosted") && ![
    "agent-host-qualification.yml",
    "native-mount-qualification.yml",
    "release-acyclic.yml",
  ].includes(name)) {
    throw new Error(`${path} uses self-hosted runners outside a native boundary workflow`);
  }
}

const expected = [
  ".github\\workflows\\publish-npm.yml:ubuntu-24.04",
  ".github\\workflows\\release-acyclic.yml:ubuntu-24.04",
];
if (process.platform !== "win32") {
  for (let index = 0; index < expected.length; index += 1) {
    expected[index] = expected[index].replaceAll("\\", "/");
  }
}
if (JSON.stringify(githubHosted) !== JSON.stringify(expected)) {
  throw new Error(`GitHub-hosted runner boundary changed: ${JSON.stringify(githubHosted)}`);
}
