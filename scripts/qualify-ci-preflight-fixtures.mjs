import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { hermeticCompilerPlans } from "./check-hermetic-clippy.mjs";
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

export function verifyModelResults(plans, results) {
  // Required executable selections must complete once per planned invocation;
  // scheduling order is not a qualification contract.
  const counts = values => {
    const result = new Map();
    for (const value of values) {
      const key = JSON.stringify(value);
      result.set(key, (result.get(key) ?? 0) + 1);
    }
    return [...result].sort(([a], [b]) => a.localeCompare(b));
  };
  assert.deepEqual(counts(results), counts(plans.flatMap(plan => plan.tests.map(test => [plan.target, test.args]))));
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
        build.tests.set(id, [test.args, `operation model; printf '%s\\n' ${quote(JSON.stringify([plan.target, test.args]))} >>${log}; `
          + `printf '%s' ${quote(modelResult(plan, test.args))}`]);
      }
    }
    builds.set(built.executable, build);
  }
  const executable = { mode: 0o755 };
  const compilerCases = hermeticCompilerPlans().map(plan => {
    // Real Clippy separately proves these compiler-resolved rejections. Here
    // execute the actual CLI's result validation with controlled compiler output.
    const messages = plan.negative ? Array.from({length: 11}, (_, index) => JSON.stringify({reason: "compiler-message", message: {
      level: "error", code: {code: "clippy::disallowed_methods"}, rendered: "fixture rejection", spans: [{is_primary: true, line_start: index + 1}],
    }}) + "\n").join("") : "";
    return [plan.args, `operation compiler-${plan.negative ? "negative" : "positive"}; printf '%s' ${quote(messages)}; exit ${plan.negative ? 1 : 0}`];
  });
  const prelude = `source ${quote(join(directory, "operation.sh"))}; `;
  writeFileSync(join(directory, "operation.sh"), `operation() {
  if [[ -s "$FAKE_OPERATION_DIR/failed" ]]; then
    printf '%s\\n' "$1" >>"$FAKE_OPERATION_DIR/downstream"
    exit 8
  fi
  printf '%s\\n' "$1" >>"$FAKE_OPERATION_DIR/operations"
  if [[ "\${FAKE_FAIL_ROLE:-}" == "$1" ]]; then
    printf '%s\\n' "$1" >"$FAKE_OPERATION_DIR/failed"
    exit 7
  fi
}
`);
  writeFileSync(join(directory, "cargo"), dispatcher([...builds.values()]
    .map(build => [build.args, `${prelude}operation build; printf '%s' ${quote(build.artifact)}`])
    .concat(compilerCases.map(([args, body]) => [args, prelude + body])), "unplanned model build"), executable);
  for (const [path, build] of builds) writeFileSync(path,
    dispatcher([...build.tests.values()].map(([args, body]) => [args, prelude + body]), "unplanned model invocation"), executable);
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
    writeFileSync(join(directory, "operations"), "");
    for (const name of ["failed", "downstream"]) writeFileSync(join(directory, name), "");
    prepare(plans, directory);
  } else if (mode === "verify") {
    const plans = JSON.parse(readFileSync(planFile, "utf8"));
    const lines = name => {
      const content = readFileSync(join(directory, name), "utf8");
      assert.ok(content.endsWith("\n"), `incomplete fixture log: ${name}`);
      return content.slice(0, -1).split("\n");
    };
    verifyModelResults(plans, lines("models.log").map(line => JSON.parse(line)));
    const operations = lines("operations");
    for (const required of ["compiler-positive", "compiler-negative", "build", "model"]) {
      assert.ok(operations.includes(required), `required qualification did not execute: ${required}`);
    }
  } else throw new Error(`unknown fixture operation: ${mode}`);
}
