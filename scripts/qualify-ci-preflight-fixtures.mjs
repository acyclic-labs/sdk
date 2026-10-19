import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { qualificationGatePlans } from "./qualify-mcp-loom.mjs";

export function artifact(plans, args, directory) {
  const index = plans.findIndex(plan => JSON.stringify(plan.args) === JSON.stringify(args));
  assert.notEqual(index, -1, `unplanned model build: ${JSON.stringify(args)}`);
  return {
    reason: "compiler-artifact", target: { name: plans[index].target },
    profile: { test: true }, executable: join(directory, `model-${index}`),
  };
}

export function modelResult(plan, args) {
  const test = plan.tests.find(test => JSON.stringify(test.args) === JSON.stringify(args));
  assert.ok(test, `unplanned model invocation: ${JSON.stringify(args)}`);
  return `test result: ok. ${test.passed ?? 6} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n`;
}

export function verifyModelCalls(plans, calls) {
  assert.deepEqual(calls, plans.flatMap(plan => plan.tests.map(test => [plan.args, test.args])));
}

// The shell-owned gate commands with the exact RUSTFLAGS each adds; the model steps come from the plan.
const loomFlags = "-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom";
const shellGate = {
  before: [
    ["check --locked -p acyclic-harness-fuzz --bins", "-Dwarnings"],
    ["check --locked -p acyclic-harness --features model-checking --lib", loomFlags],
    ["check --locked -p acyclic-harness --features filesystem-local,model-checking --lib", loomFlags],
    ["check --locked -p acyclic-harness --features filesystem --lib --tests", ""],
    ["test -p acyclic-harness --locked --features model-checking --test client-mailbox-model --test client_journal_model --test mcp-loom", loomFlags],
    ["test -p acyclic-harness --locked --features model-checking --lib workflow::model_checking::tests::", loomFlags],
  ],
  after: [
    ["nextest run --profile ci --workspace --all-features --locked", ""],
    ["test -p acyclic-harness --all-features --locked --example approved-native-process", ""],
  ],
};

export function expectedGate(plans, base = "") {
  const shell = ([command, added]) => [command, [base, added].filter(Boolean).join(" ")];
  return [
    ...shellGate.before.map(shell),
    ...plans.map(plan => [plan.args.join(" "), plan.rustflags]),
    ...shellGate.after.map(shell),
  ].map(([command, rustflags]) => [command, rustflags.trim()]);
}

export function verifyGate(plans, cargoCalls, flags, modelCalls, base = "") {
  assert.equal(flags.length, cargoCalls.length);
  assert.deepEqual(cargoCalls.map((command, index) => [command, flags[index].trim()]), expectedGate(plans, base));
  verifyModelCalls(plans, modelCalls);
}

// Compile the plan into shell dispatchers so the gate spawns no interpreter per build or model run.
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;

function dispatcher(cases, unplanned) {
  return [
    "#!/usr/bin/env bash", "IFS=$'\\x1f'", 'case "$*" in',
    ...cases.map(([args, body]) => `  ${quote(args.join("\x1f"))}) ${body} ;;`),
    `  *) { printf '%s:' ${quote(unplanned)}; printf ' %q' "$@"; echo; } >&2; exit 1 ;;`, "esac", "",
  ].join("\n");
}

function prepare(plans, directory) {
  const log = quote(join(directory, "models.log"));
  const builds = new Map();
  for (const plan of plans) {
    const built = artifact(plans, plan.args, directory);
    const build = builds.get(built.executable) ?? { args: plan.args, artifact: `${JSON.stringify(built)}\n`, tests: new Map() };
    for (const test of plan.tests) {
      const id = JSON.stringify(test.args);
      if (!build.tests.has(id)) {
        build.tests.set(id, [test.args, `printf '%s\\n' ${quote(JSON.stringify([plan.args, test.args]))} >>${log}; `
          + `printf '%s' ${quote(modelResult(plan, test.args))}`]);
      }
    }
    builds.set(built.executable, build);
  }
  const executable = { mode: 0o755 };
  writeFileSync(join(directory, "cargo"), dispatcher([...builds.values()]
    .map(build => [build.args, `printf '%s' ${quote(build.artifact)}`]), "unplanned model build"), executable);
  for (const [path, build] of builds) writeFileSync(path, dispatcher([...build.tests.values()], "unplanned model invocation"), executable);
}

const helper = fileURLToPath(import.meta.url);
if (process.argv[1] && resolve(process.argv[1]) === helper) {
  const [mode, directory] = process.argv.slice(2);
  const planFile = join(directory, "model-plans.json");
  if (mode === "prepare") {
    const plans = qualificationGatePlans();
    mkdirSync(directory, { recursive: true });
    writeFileSync(planFile, JSON.stringify(plans));
    writeFileSync(join(directory, "models.log"), "");
    prepare(plans, directory);
  } else if (mode === "verify") {
    const plans = JSON.parse(readFileSync(planFile, "utf8"));
    const lines = name => {
      const content = readFileSync(join(directory, name), "utf8");
      assert.ok(content.endsWith("\n"), `incomplete fixture log: ${name}`);
      return content.slice(0, -1).split("\n");
    };
    verifyGate(plans, lines("cargo.log"), lines("flags.log"), lines("models.log").map(line => JSON.parse(line)),
      process.env.RUSTFLAGS ?? "");
  } else throw new Error(`unknown fixture operation: ${mode}`);
}
