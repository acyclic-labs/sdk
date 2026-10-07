import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const operations = new Map([
  ["build", { stage: "write", delegate: "build", stageAfter: false }],
  ["check", { stage: "check", delegate: "check", stageAfter: false }],
  ["generate", { stage: "write", delegate: "generate", stageAfter: true }],
  ["check-generated", { stage: "check", delegate: "check:generated", stageAfter: false }],
]);
const [operation, bundleArgument, ...extra] = process.argv.slice(2);
const selected = operations.get(operation);
if (!selected || !bundleArgument || extra.length) {
  throw new Error(
    "usage: build-with-rust-actors.mjs <build|check|generate|check-generated> <generation-bundle>",
  );
}

const bundle = resolve(bundleArgument);
if (!existsSync(bundle)) throw new Error(`generation bundle is missing: ${bundle}`);

const bun = process.platform === "win32" ? "bun.exe" : "bun";
function runStage() {
  const stage = spawnSync(
    process.execPath,
    [join(root, "scripts", "stage-rust-actors-types.mjs"), selected.stage, bundle],
    { cwd: root, stdio: "inherit" },
  );
  if (stage.error) throw stage.error;
  return stage.status ?? 1;
}

function runDelegate() {
  const delegated = spawnSync(bun, ["run", selected.delegate], { cwd: root, stdio: "inherit" });
  if (delegated.error) throw delegated.error;
  return delegated.status ?? 1;
}

const first = selected.stageAfter ? runDelegate() : runStage();
if (first !== 0) process.exit(first);
process.exit(selected.stageAfter ? runStage() : runDelegate());
