import { repositoryRoot as root } from "./policy-core.mjs";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { bazelCommand } from "./bazel-executables.mjs";

const target = "//rust/crates/harness/tests/ui/native-environment:hermetic_environment_ui";
const rustcFlag = flag => `--@rules_rust//rust/settings:per_crate_rustc_flag=${target}@${flag}`;

export function hermeticCompilerPlans() {
  return [false, true].map(negative => ({
    negative,
    args: [
      "build", "--color=no", "--noshow_progress",
      "--@rules_rust//rust/settings:error_format=json",
      ...["-Dwarnings", "-Dclippy::disallowed_methods", "-Dclippy::disallowed_macros"].map(rustcFlag),
      negative ? `${target}_negative` : target,
    ],
  }));
}

export function qualifyHermeticCompiler(run = spawnSync) {
  for (const { negative, args } of hermeticCompilerPlans()) {
    const start = performance.now();
    const result = run(bazelCommand(), args, { cwd: root, encoding: "utf8", maxBuffer: Infinity });
    if (result.error) throw result.error;
    const messages = `${result.stdout}\n${result.stderr}`.split(/\r?\n/)
      .filter(line => line.trimStart().startsWith("{")).map(line => JSON.parse(line));
    const errors = messages.filter(message => message.level === "error"
      && message.message !== "aborting due to 9 previous errors");
    if (!negative) assert.equal(result.status, 0, result.stderr);
    else {
      assert.notEqual(result.status, 0, "negative control unexpectedly compiled");
      assert.equal(errors.length, 9, result.stderr);
      assert.ok(errors.every(error => ["clippy::disallowed_methods", "clippy::disallowed_macros"].includes(error.code?.code)),
        "controls failed for an unrelated compiler error");
      assert.equal(new Set(errors.flatMap(error => error.spans.filter(span => span.is_primary)
        .map(span => span.line_start))).size, 9);
    }
    console.log(`${negative ? "negative (9 compiler-resolved rejections)" : "positive (real seam)"}: ${(performance.now() - start).toFixed(0)} ms`);
  }
}

if (import.meta.main) qualifyHermeticCompiler();
