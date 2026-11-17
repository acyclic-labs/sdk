// The workspace as its checked-in manifests declare it, read without running Cargo: the members of the root
// Cargo.toml and, for each, its package name and the path dependencies it declares, of every kind and for every
// target. A TOML file is parsed by a real TOML parser (Python's tomllib), so values keep their exact form.
import path from "node:path";
import { toml } from "./policy-core.mjs";

const KINDS = [["dependencies", "normal"], ["dev-dependencies", "dev"], ["build-dependencies", "build"]];

/**
 * Every workspace member by repository-relative directory: its package name and its path dependencies on other
 * members, each with its kind. A path dependency outside the workspace refuses.
 * @param {string} root
 * @returns {Map<string, { name: string, dependencies: { directory: string, kind: string }[] }>}
 */
export function workspaceMembers(root) {
  const workspace = toml(root, "Cargo.toml").workspace ?? {};
  const crates = new Map();
  for (const directory of workspace.members ?? []) {
    const manifest = toml(root, `${directory}/Cargo.toml`);
    const tables = [manifest, ...Object.values(manifest.target ?? {})];
    // An inherited dependency's path is relative to the workspace root.
    const located = (name, spec) => spec?.workspace === true
      ? typeof workspace.dependencies?.[name]?.path === "string" ? path.posix.normalize(workspace.dependencies[name].path) : undefined
      : typeof spec?.path === "string" ? path.posix.normalize(path.posix.join(directory, spec.path)) : undefined;
    const dependencies = tables.flatMap(table => KINDS.flatMap(([section, kind]) => Object.entries(table[section] ?? {})
      .map(([name, spec]) => ({ directory: located(name, spec), kind }))
      .filter(dependency => dependency.directory !== undefined)));
    crates.set(directory, { name: manifest.package.name, dependencies });
  }
  // A path dependency outside the members is vendored source (rust/vendor), which the root manifest excludes
  // from the workspace; it is an input by its directory, not a member with its own closure.
  for (const crate of crates.values()) crate.dependencies = crate.dependencies.filter(dependency => crates.has(dependency.directory));
  return crates;
}

/** The member directory of a package name. @param {string} root @param {string} name */
export function memberDirectory(root, name) {
  const found = [...workspaceMembers(root)].filter(([, crate]) => crate.name === name);
  if (found.length !== 1) throw new Error(`STEP_FAILED workspace-manifests: ${found.length} workspace members are named ${name}`);
  return found[0][0];
}
