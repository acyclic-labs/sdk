import { appendFileSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function parseGeneratorBackends(value) {
  if (!Array.isArray(value) || value.length === 0
    || value.some(entry => !entry || typeof entry.name !== "string" || !/^[a-z][a-z0-9-]*$/.test(entry.name)
      || Object.keys(entry).some(key => key !== "name"))
    || new Set(value.map(entry => entry.name)).size !== value.length) {
    throw new Error("invalid SDK generator backend registry");
  }
  return Object.freeze(value.map(entry => Object.freeze({ name: entry.name })));
}
const generatorRegistry = parseGeneratorBackends(JSON.parse(
  readFileSync(new URL("../.github/sdk-generator-backends.json", import.meta.url), "utf8"),
));
export const languageGeneratorBackends = Object.freeze(generatorRegistry.map(entry => entry.name));

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] !== "select") throw new Error("usage: plan-qualification.mjs select");
  const matrix = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
  const output = "matrix=" + JSON.stringify(matrix) + "\n";
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, output);
  process.stdout.write(output);
}
