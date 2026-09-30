import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const mode = process.argv[2] ?? "write";
if (mode !== "write" && mode !== "check") throw new Error("expected write or check");

for (const family of ["actors", "workers"]) {
  const run = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", `acyclic-${family}`, "--example", `${family}-http-routes`], {
    cwd: root, encoding: "utf8",
  });
  if (run.status !== 0) throw new Error(run.stderr || `${family} route generator failed`);
  const routes = Object.fromEntries(JSON.parse(run.stdout));
  const output = `// Generated from acyclic-${family}::HTTP_ROUTES. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(routes, null, 2)} as const;\n`;
  const path = join(root, "typescript", "packages", family, "src", "routes.ts");
  if (mode === "check") {
    if (readFileSync(path, "utf8") !== output) throw new Error(`${family} HTTP route drift`);
  } else {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, output);
  }
}

const moduleContract = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-workers", "--example", "workers-module-contract"], {
  cwd: root, encoding: "utf8",
});
if (moduleContract.status !== 0) throw new Error(moduleContract.stderr || "Workers module contract generation failed");
const modulePath = join(root, "typescript", "packages", "workers", "src", "module-contract.ts");
if (mode === "check") {
  if (readFileSync(modulePath, "utf8") !== moduleContract.stdout) throw new Error("Workers module contract drift");
} else {
  writeFileSync(modulePath, moduleContract.stdout);
}
