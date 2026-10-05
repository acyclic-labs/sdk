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
    if (match) githubHosted.push(`${path.replaceAll("\\", "/")}:${match[2]}-${match[3]}`);
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

// Qualification lanes are scheduled from data rather than workflow YAML.
for (const lane of JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"))) {
  if (typeof lane.runner !== "string" || !/^blacksmith-\d+vcpu-[a-z0-9-]+$/.test(lane.runner)) {
    throw new Error(`qualification lane ${lane.lane} must run on a Blacksmith runner, not ${lane.runner}`);
  }
}

// Exact release/manual package runner boundary. The PR qualification lanes
// remain Blacksmith-only above; native package proofs also need ARM Windows,
// Darwin, and explicit host images matching their installed consumers.
const expected = [
  ".github/workflows/dotnet-embedded-rid-manual.yml:windows-11-vs2026-arm",
  ".github/workflows/dotnet-embedded-rid-manual.yml:macos-15-intel",
  ".github/workflows/dotnet-embedded-rid-manual.yml:windows-11-vs2026-arm",
  ".github/workflows/dotnet-embedded-rid-manual.yml:macos-15-intel",
  ".github/workflows/dotnet-native-rid-manual.yml:ubuntu-24.04",
  ".github/workflows/dotnet-native-rid-manual.yml:ubuntu-24.04-arm",
  ".github/workflows/dotnet-native-rid-manual.yml:ubuntu-24.04",
  ".github/workflows/dotnet-native-rid-manual.yml:ubuntu-24.04-arm",
  ".github/workflows/dotnet-native-rid-manual.yml:macos-14",
  ".github/workflows/dotnet-native-rid-manual.yml:macos-14",
  ".github/workflows/dotnet-native-rid-manual.yml:windows-2025",
  ".github/workflows/dotnet-native-rid-manual.yml:windows-2025",
  ".github/workflows/embedded-abi-release.yml:windows-11-vs2026-arm",
  ".github/workflows/embedded-native-packaging.yml:windows-11-vs2026-arm",
  ".github/workflows/embedded-native-packaging.yml:macos-15-intel",
  ".github/workflows/embedded-native-packaging.yml:macos-15",
  ".github/workflows/embedded-native-packaging.yml:windows-11-vs2026-arm",
  ".github/workflows/embedded-native-packaging.yml:macos-15-intel",
  ".github/workflows/embedded-native-packaging.yml:macos-15",
  ".github/workflows/embedded-native-packaging.yml:windows-11-vs2026-arm",
  ".github/workflows/embedded-native-packaging.yml:macos-15-intel",
  ".github/workflows/embedded-native-packaging.yml:macos-15",
  ".github/workflows/embedded-native-packaging.yml:windows-11-vs2026-arm",
  ".github/workflows/embedded-native-packaging.yml:macos-15-intel",
  ".github/workflows/embedded-native-packaging.yml:macos-15",
  ".github/workflows/http-target-release-qualification.yml:ubuntu-24.04",
  ".github/workflows/publish-crate.yml:ubuntu-24.04",
  ".github/workflows/publish-npm.yml:ubuntu-24.04",
  ".github/workflows/python-go-release-qualification.yml:ubuntu-24.04",
  ".github/workflows/rpd-live-consumers.yml:ubuntu-latest",
  ".github/workflows/rpd-live-consumers.yml:windows-latest",
  ".github/workflows/stream-native-packages.yml:macos-15-intel",
  ".github/workflows/stream-native-packages.yml:macos-15",
  ".github/workflows/stream-native-packages.yml:windows-11-vs2026-arm"
];
if (JSON.stringify(githubHosted) !== JSON.stringify(expected)) {
  throw new Error(`GitHub-hosted runner boundary changed: ${JSON.stringify(githubHosted)}`);
}
