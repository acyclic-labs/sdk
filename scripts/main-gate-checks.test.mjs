import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const runner = join(dirname(fileURLToPath(import.meta.url)), "main-gate-checks.sh");
test("legacy coordinator requires the shared formatter before fallback phases", { skip: process.platform === "win32" }, () => {
  const cwd = mkdtempSync(join(tmpdir(), "main-gate-format-fallback-"));
  try {
    mkdirSync(join(cwd, "bin"));
    mkdirSync(join(cwd, "scripts"));
    writeFileSync(join(cwd, "scripts/gate-rust-format.sh"), readFileSync(join(dirname(runner), "gate-rust-format.sh")));
    writeFileSync(join(cwd, "bin/bun"), '#!/bin/bash\nexit 101\n');
    chmodSync(join(cwd, "bin/bun"), 0o755);
    const source = readFileSync(join(dirname(runner), "buildbox/coordinator/gate-checks.sh"), "utf8");
    const marker = "cat > .gate-feature-matrix.mjs <<'GATE_MATRIX_BOOTSTRAP'";
    assert.ok(source.includes(marker));
    const prefix = source.slice(0, source.indexOf(marker));
    const hook = 'bash scripts/gate-rust-format.sh || exit $?';
    assert.ok(prefix.includes(hook));
    // End after the real fallback's prerequisite; all later phases succeed.
    for (const negative of [false, true]) {
      const selected = join(cwd, "runner.sh");
      writeFileSync(selected, (negative ? prefix.replace(hook, ":") : prefix) + '\n)\ngate_main_existing "$@"\n');
      const result = spawnSync("bash", [selected], { cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}` } });
      assert.equal(result.status, negative ? 0 : 101, result.stdout + result.stderr);
    }
  } finally { rmSync(cwd, { recursive: true, force: true }); }
});

test("main covers targets and docs, retains errors and never retries failures", { skip: process.platform === "win32" }, () => {
  const cwd = mkdtempSync(join(tmpdir(), "main-gate-"));
  try {
    mkdirSync(join(cwd, "bin"));
    mkdirSync(join(cwd, "scripts"));
    writeFileSync(join(cwd, "scripts/gate-rust-format.sh"), readFileSync(join(dirname(runner), "gate-rust-format.sh")));
    writeFileSync(join(cwd, "bin/bun"), '#!/bin/bash\necho "format $*" >> calls\nexit "${FORMAT_RC:-0}"\n');
    chmodSync(join(cwd, "bin/bun"), 0o755);
    writeFileSync(join(cwd, "scripts/gate-test-runner.sh"), readFileSync(join(dirname(runner), "gate-test-runner.sh")));
    const cargo = join(cwd, "bin", "cargo");
    writeFileSync(cargo, `#!/bin/bash
echo "$*" >> calls
case "$1" in
metadata) echo '{"workspace_members":["a","b"],"packages":[{"id":"a","name":"harness"},{"id":"b","name":"examples"}]}' ;;
test)
  if [[ " $* " == *" --no-run "* ]]; then
    if [ "$BUILD_FAIL" = 1 ]; then echo 'error[E0061]: diagnostic on stderr' >&2; exit 101; fi
    exit 0
  fi
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
    writeFileSync(join(cwd, "scripts/gate-windows-features.sh"), '#!/bin/bash\necho "windows-features $*" >> calls\nexit "${WINDOWS_FEATURE_RC:-0}"\n');
    writeFileSync(join(cwd, "scripts/check-browser-targets.sh"), '#!/bin/bash\necho "browser target check: fixture"\necho "$CARGO_TARGET_DIR" > browser-target\necho "browser" >> calls\nif [ "$BROWSER_RC" != 0 ]; then echo "error: browser diagnostic on stderr" >&2; fi\nexit "$BROWSER_RC"\n');
    writeFileSync(join(cwd, "scripts/gate-feature-matrix.sh"), "#!/bin/bash\necho 'feature matrix: 104/104 sets ok in 1.0s'\nexit \"$FEATURE_RC\"\n");
    writeFileSync(join(cwd, "scripts/check-harness-models.sh"), "#!/bin/bash\nexit 0\n");
    for (const [target, doc, features, browser, expected, early = false, build = false] of [[0, 0, 0, 0, 0], [101, 0, 0, 0, 101], [0, 101, 0, 0, 101], [0, 0, 1, 0, 1], [0, 0, 0, 101, 101], [101, 0, 0, 0, 101, true], [101, 0, 0, 0, 101, false, true]]) {
      writeFileSync(join(cwd, "calls"), "");
      const result = spawnSync("bash", [runner, "--each-feature", "--locked", "-p", "harness", "--all-features", "--no-fail-fast", "--", "--skip", "known-hang"], {
        cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_MAIN_TARGET_DIR: join(cwd, "cache"), EARLY_FAIL: early ? "1" : "0", BUILD_FAIL: build ? "1" : "0", TARGET_RC: String(target), DOC_RC: String(doc), FEATURE_RC: String(features), BROWSER_RC: String(browser), WINDOWS_FEATURE_RC: "0" },
      });
      assert.equal(result.status, expected, result.stdout + result.stderr);
      const tests = readFileSync(join(cwd, "calls"), "utf8").trim().split("\n").filter(line => line.startsWith("test "));
      assert.equal(tests.length, build ? 2 : 3, "one attempt per phase, including after failure");
      assert.equal(readFileSync(join(cwd, "calls"), "utf8").split("\n").filter(line => line === "windows-features full").length, 1);
      assert.match(result.stdout, new RegExp(`GATE_TEST_WALL phase=targets seconds=\\d+ rc=${target}`));
      assert.match(result.stdout, new RegExp(`GATE_TEST_WALL phase=docs seconds=\\d+ rc=${doc}`));
      assert.equal(readFileSync(join(cwd, "calls"), "utf8").trim().split("\n").filter(line => line === "browser").length, 1, "browser runs once, including after earlier failures");
      assert.equal(readFileSync(join(cwd, "browser-target"), "utf8").trim(), join(cwd, "cache/browser"));
      if (browser) assert.match(result.stdout, /error: browser diagnostic on stderr/u);
      assert.match(result.stdout, new RegExp(`GATE_BROWSER_WALL seconds=\\d+ rc=${browser}`));
      if (!build) assert.match(tests[1], /--workspace .*--all-targets -- --skip known-hang/u);
      assert.match(tests.at(-1), /--workspace .*--doc -- --skip known-hang/u);
      assert.ok(tests.every(line => !line.includes("-p harness")), "legacy roots cannot narrow main");
      assert.ok(tests.every(line => !line.includes("--keep-going")), "cargo test has no keep-going option");
      if (target || doc) assert.doesNotMatch(result.stdout, /GATE_TEST_COVERAGE all-targets\+doc/u);
      else assert.match(result.stdout, /GATE_TEST_COVERAGE all-targets\+doc packages=examples,harness/u);
      if (target) assert.doesNotMatch(result.stdout, /GATE_TEST_COVERAGE phase=targets/u);
      else assert.match(result.stdout, /GATE_TEST_COVERAGE phase=targets packages=examples,harness/u);
      if (doc) assert.doesNotMatch(result.stdout, /GATE_TEST_COVERAGE phase=docs/u);
      else assert.match(result.stdout, /GATE_TEST_COVERAGE phase=docs packages=examples,harness/u);
      if (target || doc) assert.match(result.stdout, /error\[E0061\]: diagnostic on stderr/u);
      assert.match(result.stdout, new RegExp(`GATE_DONE rc=${expected}`));
    }
    const windowsFailure = spawnSync("bash", [runner], {
      cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_MAIN_TARGET_DIR: join(cwd, "cache"), EARLY_FAIL: "0", TARGET_RC: "0", DOC_RC: "0", FEATURE_RC: "0", BROWSER_RC: "0", WINDOWS_FEATURE_RC: "101" },
    });
    assert.equal(windowsFailure.status, 101, windowsFailure.stdout + windowsFailure.stderr);
    assert.match(windowsFailure.stdout, /GATE_DONE rc=101/u);
    const formatHook = 'bash scripts/gate-rust-format.sh || exit $?';
    const source = readFileSync(runner, "utf8");
    assert.ok(source.includes(formatHook));
    for (const negative of [false, true]) {
      const selected = join(cwd, "format-runner.sh");
      writeFileSync(selected, negative ? source.replace(formatHook, ":") : source);
      writeFileSync(join(cwd, "calls"), "");
      const result = spawnSync("bash", [selected], {
        cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_MAIN_TARGET_DIR: join(cwd, "cache"), FORMAT_RC: "101", TARGET_RC: "0", DOC_RC: "0", FEATURE_RC: "0", BROWSER_RC: "0", WINDOWS_FEATURE_RC: "0", EARLY_FAIL: "0", BUILD_FAIL: "0" },
      });
      assert.equal(result.status, negative ? 0 : 101, result.stdout + result.stderr);
      const calls = readFileSync(join(cwd, "calls"), "utf8").trim().split("\n");
      if (!negative) {
        assert.deepEqual(calls, ["format scripts/qualify-rustfmt.mjs"], "format drift stops qualification before any compiler phase");
        assert.match(result.stdout, /GATE_RUSTFMT rc=101/u);
      }
    }
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});
