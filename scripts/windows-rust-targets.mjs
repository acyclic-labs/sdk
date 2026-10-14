import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const libraryKinds = new Set(["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"]);

/**
 * @param {{workspace_members: string[], packages: {id: string, name: string, targets: {name: string, kind: string[], test: boolean}[]}[]}} metadata
 * @param {string} shard
 */
export function windowsRustTargets(metadata, shard) {
  if (shard !== "units" && shard !== "scenarios") throw new Error("unknown Windows Rust shard");
  const members = new Set(metadata.workspace_members);
  const packages = metadata.packages.filter(pkg => members.has(pkg.id));
  if (members.size !== metadata.workspace_members.length || packages.length !== members.size
    || new Set(packages.map(pkg => pkg.id)).size !== members.size)
    throw new Error("ambiguous workspace package inventory");
  const targets = [];
  /** @type {Record<string, Set<string>>} */
  const enabled = { bin: new Set(), test: new Set(), example: new Set() };
  /** @type {Record<string, Set<string>>} */
  const disabled = { bin: new Set(), test: new Set(), example: new Set() };
  let normalExamples = false;
  for (const pkg of packages) for (const target of pkg.targets) {
    if (!target.test && !target.kind.includes("example")) {
      if (target.kind.length === 1 && ["bin", "test"].includes(target.kind[0]))
        disabled[target.kind[0]].add(target.name);
      continue;
    }
    const library = target.kind.length > 0 && target.kind.every(kind => libraryKinds.has(kind));
    if (!library && (target.kind.length !== 1 || !["bin", "test", "example"].includes(target.kind[0])))
      throw new Error(`unsupported test target: ${pkg.name}/${target.name}`);
    if (!library) {
      const kind = target.kind[0];
      (target.test ? enabled : disabled)[kind].add(target.name);
      if (kind === "example" && !target.test) normalExamples = true;
    }
    if ((shard === "units") !== library) continue;
    targets.push(`${pkg.name}/${target.kind.join("+")}/${target.name}`);
  }
  if (targets.length === 0 || new Set(targets).size !== targets.length) throw new Error("empty or duplicate test targets");
  const cargoArgs = ["--workspace", "--all-features"];
  if (shard === "units") cargoArgs.push("--lib");
  else {
    for (const kind of ["bin", "test", "example"]) for (const name of [...enabled[kind]].sort()) {
      if (disabled[kind].has(name)) throw new Error(`test-enabled and disabled targets share a name: ${kind}/${name}`);
      cargoArgs.push(`--${kind}`, name);
    }
    if (cargoArgs.length === 2) throw new Error("empty scenario test selection");
  }
  return { cargoArgs, targets: targets.sort(),
    exampleBuildArgs: shard === "scenarios" && normalExamples ? ["--workspace", "--all-features", "--examples"] : [] };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.stdout.write(JSON.stringify(windowsRustTargets(JSON.parse(readFileSync(0, "utf8")), process.argv[2] ?? "")) + "\n");
}
