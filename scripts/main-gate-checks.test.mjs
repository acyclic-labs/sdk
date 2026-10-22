import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const runner = join(dirname(fileURLToPath(import.meta.url)), "main-gate-checks.sh");
test("main covers targets and docs, retains errors and never retries failures", { skip: process.platform === "win32" }, () => {
  const cwd = mkdtempSync(join(tmpdir(), "main-gate-"));
  try {
    mkdirSync(join(cwd, "bin"));
    mkdirSync(join(cwd, "scripts"));
    writeFileSync(join(cwd, "scripts/gate-test-runner.sh"), readFileSync(join(dirname(runner), "gate-test-runner.sh")));
    const cargo = join(cwd, "bin", "cargo");
    writeFileSync(cargo, `#!/bin/bash
echo "$*" >> calls
case "$1" in
metadata) echo '{"workspace_members":["a","b"],"packages":[{"id":"a","name":"harness"},{"id":"b","name":"examples"}]}' ;;
test)
  if [[ " $* " == *" --doc "* ]]; then rc=$DOC_RC; else rc=$TARGET_RC; fi
  if [ "$rc" != 0 ]; then echo 'error[E0061]: diagnostic on stderr' >&2; fi
  if [ "$EARLY_FAIL" = 1 ] && [[ " $* " == *" --all-targets "* ]]; then exit "$rc"; fi
  echo "test result: fixture rc=$rc"
  exit "$rc" ;;
clippy) exit 0 ;;
*) exit 9 ;;
esac
`);
    chmodSync(cargo, 0o755);
    writeFileSync(join(cwd, "scripts/check-browser-targets.sh"), '#!/bin/bash\necho "browser target check: fixture"\necho "$CARGO_TARGET_DIR" > browser-target\necho "browser" >> calls\nif [ "$BROWSER_RC" != 0 ]; then echo "error: browser diagnostic on stderr" >&2; fi\nexit "$BROWSER_RC"\n');
    writeFileSync(join(cwd, "scripts/gate-feature-matrix.sh"), "#!/bin/bash\necho 'feature matrix: 104/104 sets ok in 1.0s'\nexit \"$FEATURE_RC\"\n");
    for (const [target, doc, features, browser, expected, early = false] of [[0, 0, 0, 0, 0], [101, 0, 0, 0, 101], [0, 101, 0, 0, 101], [0, 0, 1, 0, 1], [0, 0, 0, 101, 101], [101, 0, 0, 0, 101, true]]) {
      writeFileSync(join(cwd, "calls"), "");
      const result = spawnSync("bash", [runner, "--each-feature", "--locked", "-p", "harness", "--all-features", "--no-fail-fast", "--", "--skip", "known-hang"], {
        cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_MAIN_TARGET_DIR: join(cwd, "cache"), EARLY_FAIL: early ? "1" : "0", TARGET_RC: String(target), DOC_RC: String(doc), FEATURE_RC: String(features), BROWSER_RC: String(browser) },
      });
      assert.equal(result.status, expected, result.stdout + result.stderr);
      const tests = readFileSync(join(cwd, "calls"), "utf8").trim().split("\n").filter(line => line.startsWith("test "));
      assert.equal(tests.length, 2, "one attempt per phase, including after failure");
      assert.equal(readFileSync(join(cwd, "calls"), "utf8").trim().split("\n").filter(line => line === "browser").length, 1, "browser runs once, including after earlier failures");
      assert.equal(readFileSync(join(cwd, "browser-target"), "utf8").trim(), join(cwd, "cache/browser"));
      if (browser) assert.match(result.stdout, /error: browser diagnostic on stderr/u);
      assert.match(result.stdout, new RegExp(`GATE_BROWSER_WALL seconds=\\d+ rc=${browser}`));
      assert.match(tests[0], /--workspace .*--all-targets -- --skip known-hang/u);
      assert.match(tests[1], /--workspace .*--doc -- --skip known-hang/u);
      assert.ok(tests.every(line => !line.includes("-p harness")), "legacy roots cannot narrow main");
      assert.ok(tests.every(line => !line.includes("--keep-going")), "cargo test has no keep-going option");
      if (early) assert.doesNotMatch(result.stdout, /GATE_TEST_COVERAGE (all-targets|phase=targets)/u);
      else assert.match(result.stdout, /GATE_TEST_COVERAGE all-targets\+doc packages=examples,harness/u);
      assert.match(result.stdout, /GATE_TEST_COVERAGE phase=docs packages=examples,harness/u);
      if (target || doc) assert.match(result.stdout, /error\[E0061\]: diagnostic on stderr/u);
      assert.match(result.stdout, new RegExp(`GATE_DONE rc=${expected}`));
    }
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});

test("only fuzz binaries receive bounded deterministic inputs, retaining the outer runner", { skip: process.platform === "win32" }, () => {
  const cwd = mkdtempSync(join(tmpdir(), "gate-runner-"));
  try {
    const bins = join(cwd, "fuzz-bins");
    writeFileSync(bins, "access\n");
    const outer = join(cwd, "timeout-runner");
    writeFileSync(outer, '#!/bin/bash\nprintf "%s\\n" "$@"\n');
    chmodSync(outer, 0o755);
    const invoke = program => spawnSync("bash", [join(dirname(runner), "gate-test-runner.sh"), program, "--skip", "known-hang"], {
      cwd, encoding: "utf8", env: { ...process.env, GATE_FUZZ_TEST_BIN_FILE: bins, GATE_BASE_TEST_RUNNER: outer },
    });
    const fuzz = invoke("/target/debug/deps/access-abc123");
    assert.equal(fuzz.status, 0, fuzz.stderr);
    assert.match(fuzz.stdout, /-runs=1\n-seed=1\n-max_total_time=10/u);
    assert.ok(!fuzz.stdout.includes("--skip"));
    const ordinary = invoke("/target/debug/deps/normal-abc123");
    assert.equal(ordinary.status, 0, ordinary.stderr);
    assert.equal(ordinary.stdout, "/target/debug/deps/normal-abc123\n--skip\nknown-hang\n");
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});
