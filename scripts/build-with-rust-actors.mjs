import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const operations = new Map([
  ["drift", { stage: null, delegate: null }],
  ["build", { stage: "write", delegate: "build" }],
  ["check", { stage: "check", delegate: "check" }],
  ["generate", { stage: "write", delegate: "generate" }],
  ["check-generated", { stage: "check", delegate: "check:generated" }],
]);
const [operation, bundleArgument, ...extra] = process.argv.slice(2);
const selected = operations.get(operation);
if (
  !selected ||
  !bundleArgument ||
  (extra.length !== 0 && (extra.length !== 2 || extra[0] !== "--rustdoc-json"))
) {
  throw new Error(
    "usage: build-with-rust-actors.mjs <drift|build|check|generate|check-generated> <generation-bundle> [--rustdoc-json <path>]",
  );
}

const bundle = resolve(bundleArgument);
if (!existsSync(bundle)) throw new Error(`generation bundle is missing: ${bundle}`);
const manifest = JSON.parse(readFileSync(join(bundle, "generation-manifest.json"), "utf8"));
if (typeof manifest.version !== "string" || !manifest.version) {
  throw new Error("generation manifest is missing version");
}
if (!manifest.tool || !["release", "preview"].includes(manifest.tool.channel)) {
  throw new Error("generation manifest has an invalid documentation channel");
}
const rustdocJson = extra.length ? resolve(extra[1]) : undefined;
if (manifest.tool.channel === "preview" && !rustdocJson) {
  throw new Error("preview generation bundles require --rustdoc-json <path>");
}
if (manifest.tool.channel === "release" && rustdocJson) {
  throw new Error("release generation bundles must not receive --rustdoc-json");
}

const bun = process.platform === "win32" ? "bun.exe" : "bun";
function runDrift() {
  const args = [
    "+1.98.1",
    "run",
    "--offline",
    "--locked",
    "--manifest-path",
    join(root, "rust/crates/sdk-generation/Cargo.toml"),
    "--",
    "drift",
    "--root",
    root,
    "--output",
    bundle,
    "--version",
    manifest.version,
    "--channel",
    manifest.tool.channel,
  ];
  if (rustdocJson) args.push("--rustdoc-json", rustdocJson);
  const drift = spawnSync("cargo", args, { cwd: root, stdio: "inherit" });
  if (drift.error) throw drift.error;
  return drift.status ?? 1;
}

function runStage(operation) {
  const stage = spawnSync(
    process.execPath,
    [join(root, "scripts", "stage-rust-actors-types.mjs"), operation, bundle],
    { cwd: root, stdio: "inherit" },
  );
  if (stage.error) throw stage.error;
  return stage.status ?? 1;
}

function runWorkersStage(operation) {
  return runStage(operation === "write" ? "contract-write" : "contract-check");
}

function runDelegate() {
  const delegated = spawnSync(bun, ["run", selected.delegate], { cwd: root, stdio: "inherit" });
  if (delegated.error) throw delegated.error;
  return delegated.status ?? 1;
}

const drift = runDrift();
if (drift !== 0) process.exit(drift);
if (operation === "drift") process.exit(0);
if (operation === "generate") {
  const workers = runWorkersStage("write");
  if (workers !== 0) process.exit(workers);
  const delegated = runDelegate();
  if (delegated !== 0) process.exit(delegated);
  process.exit(runStage("write"));
}

const workers = runWorkersStage(selected.stage);
if (workers !== 0) process.exit(workers);
const actors = runStage(selected.stage);
if (actors !== 0) process.exit(actors);
process.exit(runDelegate());
