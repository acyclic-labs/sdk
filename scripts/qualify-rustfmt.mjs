// Formatting gate for every tracked Rust file: root-workspace members and the
// standalone fuzz, proof, model and binding crates that `cargo fmt --all` never
// visits. Each file is checked under the edition of its nearest Cargo.toml, as
// `cargo fmt` would. Vendored third-party crates are not ours to format.
import { execFileSync, spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const VENDORED = ":(exclude)rust/vendor/**";

// Returns {workspace, edition} for one manifest: `workspace` is the
// [workspace.package] edition (or null when the manifest is no workspace root),
// `edition` the package edition, "inherit", or null for a virtual manifest.
export function parseManifest(text) {
  let section = "", hasPackage = false, edition, workspace = null, isWorkspace = false;
  for (const raw of text.split("\n")) {
    const line = raw.replace(/#.*/, "").trim();
    const header = line.match(/^\[([^\]]+)\]$/);
    if (header) {
      section = header[1].trim();
      hasPackage ||= section === "package";
      isWorkspace ||= section === "workspace" || section.startsWith("workspace.");
      continue;
    }
    const value = line.match(/^edition\s*=\s*"(\d{4})"$/)?.[1];
    const inherits = /^edition\s*(?:\.\s*workspace\s*=\s*true|=\s*\{\s*workspace\s*=\s*true\s*\})$/.test(line);
    if (section === "package" && (value || inherits)) edition = value ?? "inherit";
    if (section === "workspace.package" && value) workspace = value;
  }
  return { workspace: isWorkspace ? workspace ?? "2015" : null, edition: hasPackage ? edition ?? "2015" : null };
}

const nearest = (path, table) => {
  for (let dir = dirname(path); ; dir = dirname(dir)) {
    if (table.has(dir)) return dir;
    if (dir === ".") return null;
  }
};

// Groups the tracked Rust files under `root` by the edition cargo would format them with.
export function filesByEdition(root) {
  const list = (pattern) => execFileSync("git", ["ls-files", "-z", "--", pattern, VENDORED], { cwd: root, encoding: "utf8" })
    .split("\0").filter(Boolean);
  const manifests = new Map(list("*Cargo.toml").filter((path) => /(^|\/)Cargo\.toml$/.test(path))
    .map((path) => [dirname(path), parseManifest(readFileSync(resolve(root, path), "utf8"))]));
  const workspaceEdition = (dir) => {
    for (let at = dir; at !== null; at = at === "." ? null : nearest(at, manifests)) {
      const { workspace } = manifests.get(at);
      if (workspace) return workspace;
    }
    throw new Error(`${dir}/Cargo.toml inherits an edition but has no enclosing workspace`);
  };
  const groups = new Map();
  for (const file of list("*.rs")) {
    const dir = nearest(file, manifests);
    if (dir === null) throw new Error(`${file} has no enclosing Cargo.toml`);
    const { edition, workspace } = manifests.get(dir);
    const resolved = edition === "inherit" ? workspaceEdition(dir) : edition ?? workspace ?? "2015";
    groups.set(resolved, [...(groups.get(resolved) ?? []), file]);
  }
  return groups;
}

// Checks every file exactly once: `skip_children` stops a crate root from
// re-formatting its whole module tree, and the files are spread over one batch
// per CPU (at most 200 paths each, within the Windows command-line limit).
// rustfmt reads the nearest rustfmt.toml of each file. Resolves to
// {status, output}; status is 0 only when every batch exits 0 and prints nothing.
export async function qualifyRustfmt(root) {
  const groups = filesByEdition(root);
  const total = [...groups.values()].reduce((sum, files) => sum + files.length, 0);
  if (total === 0) return { status: 1, output: "no tracked Rust files found\n" };
  const size = Math.min(200, Math.ceil(total / availableParallelism()));
  const jobs = [];
  for (const [edition, files] of groups) {
    for (let i = 0; i < files.length; i += size) {
      jobs.push(["--check", "--color", "never", "--edition", edition, "--config", "skip_children=true", ...files.slice(i, i + size)]);
    }
  }
  let status = 0, output = "", next = 0;
  const check = (args) => new Promise((done) => {
    const child = spawn("rustfmt", args, { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
    let text = "";
    child.stdout.on("data", (chunk) => { text += chunk; });
    child.stderr.on("data", (chunk) => { text += chunk; });
    child.on("error", (error) => done({ code: 1, text: `${text}${error.message}\n` }));
    child.on("close", (code) => done({ code: code ?? 1, text }));
  });
  const worker = async () => {
    while (next < jobs.length) {
      const result = await check(jobs[next++]);
      if (result.code !== 0 || result.text !== "") status = 1;
      output += result.text;
    }
  };
  await Promise.all(Array.from({ length: Math.min(availableParallelism(), jobs.length) }, worker));
  return { status, output };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { status, output } = await qualifyRustfmt(resolve(dirname(fileURLToPath(import.meta.url)), ".."));
  process.stdout.write(output);
  process.exitCode = status;
}
