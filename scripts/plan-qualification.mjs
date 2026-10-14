import { appendFileSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function parseGeneratorBackends(value) {
  if (!Array.isArray(value) || value.length === 0
    || value.some(entry => !entry || typeof entry.name !== "string" || !/^[a-z][a-z0-9-]*$/.test(entry.name)
      || Object.keys(entry).some(key => !["name", "shared"].includes(key)) || !Array.isArray(entry.shared)
      || new Set(entry.shared).size !== entry.shared.length || entry.shared.some(name => !["authority", "archive"].includes(name)))
    || new Set(value.map(entry => entry.name)).size !== value.length) {
    throw new Error("invalid SDK generator backend registry");
  }
  return Object.freeze(value.map(entry => Object.freeze({ name: entry.name, shared: Object.freeze([...entry.shared]) })));
}
const generatorRegistry = parseGeneratorBackends(JSON.parse(
  readFileSync(new URL("../.github/sdk-generator-backends.json", import.meta.url), "utf8"),
));
export const languageGeneratorBackends = Object.freeze(generatorRegistry.map(entry => entry.name));

export function selectGeneratorBackends(changed, registry = generatorRegistry) {
  const all = changed.some(path => [".github/workflows/sdk-generator.yml", ".github/sdk-generator-backends.json",
    "scripts/plan-qualification.mjs", "scripts/test-plan-qualification.mjs"].includes(path));
  return registry.filter(backend => all || changed.some(path =>
    (path.startsWith(`tools/sdk-generator/backends/${backend.name}/`) && path !== `tools/sdk-generator/backends/${backend.name}/README.md`)
    || (backend.shared.includes("authority") && path.startsWith("tools/sdk-generator/shared/"))
    || (backend.shared.includes("archive") && path === "scripts/archive-utils.mjs"))).map(backend => backend.name);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] !== "select") throw new Error("usage: plan-qualification.mjs select");
  const matrix = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
  const output = "matrix=" + JSON.stringify(matrix) + "\n";
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, output);
  process.stdout.write(output);
}
