import assert from "node:assert/strict";
import test from "node:test";
import { qualification, qualified } from "./qualification.mjs";
import { dirname, join } from "node:path";
import { mkdir, mkdtemp, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

test("external acceptance requires executed tests, not a successful empty or ignored selection", () => {
  const result = stdout => ({ code: 0, stdout, stderr: "" });
  const summary = (passed, failed = 0, ignored = 0) => `test result: ok. ${passed} passed; ${failed} failed; ${ignored} ignored;`;
  qualified(result(summary(1)), "libtest", 1);
  for (const output of ["", summary(0), summary(1, 1), summary(1, 0, 1), summary(1) + summary(1)]) {
    assert.throws(() => qualified(result(output), "libtest", 1));
  }
  qualified(result("1 pass\n0 fail"), "bun", 1);
  for (const output of ["0 pass\n0 fail", "1 pass\n1 fail", "1 pass\n0 fail\n1 skip", "1 pass\n0 fail\n1 fail", "1 pass\n1 pass\n0 fail"]) {
    assert.throws(() => qualified(result(output), "bun", 1));
  }
  const node = "# tests 1\n# pass 1\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n";
  qualified(result(node), "node", 1);
  for (const output of ["", node.replaceAll("1", "0"), node + node,
    node.replace("# pass 1", "# pass 0"),
    ...["fail", "cancelled", "skipped", "todo"].map(key => node.replace(`# ${key} 0`, `# ${key} 1`))]) {
    assert.throws(() => qualified(result(output), "node", 1));
  }
});

test("installed-host entry points receive the same prebuilt plugin and fixture profile", async () => {
  const fixture = { workspace: "fixture-workspace", env: { E2E_BUN: process.execPath,
    E2E_PLUGIN_BINARY: process.execPath, CODEX_HOME: "fixture-profile" } };
  let calls = 0, captures = 0;
  const processes = { async runOwned(binary, args, options) {
    calls++;
    assert.equal(binary, process.execPath);
    assert.equal(options.cwd, fixture.workspace);
    assert.equal(options.scopeMountRoot, dirname(fixture.workspace));
    assert.equal(options.env.CODEX_HOME, "fixture-profile");
    assert.equal(options.env.ACYCLIC_TEST_BINARY, process.execPath);
    assert.equal(options.env.ACYCLIC_BINARY, process.execPath);
    assert.equal(options.env.RUST_TEST_THREADS, "1");
    assert.equal(options.env.ACYCLIC_TEST_FIXTURE_ROOT, options.scopeMountRoot);
    return { code: 0, stdout: "1 pass\n0 fail", stderr: "" };
  } };
  const input = { binary_env: "E2E_BUN", fixture_binary_env: "E2E_PLUGIN_BINARY",
    args: ["test"], format: "bun", minimum_tests: 1 };
  await qualification(processes, input, fixture, () => captures++);
  assert.equal(calls, 1); assert.equal(captures, 1);
  await assert.rejects(qualification(processes, { ...input, fixture_binary_env: "ABSENT" }, fixture, () => {}), /prebuilt plugin/);
  assert.equal(calls, 1, "missing fixture must fail before launching a host");
});

test("installed hosts are refused before launch without an admitted host executable", async () => {
  const processes = { async runOwned() { assert.fail("a refused host launched a process"); } };
  const fixture = { workspace: "/fixture/workspace", env: { E2E_PI: "relative/pi" } };
  await assert.rejects(qualification(processes, { format: "installed-host", host: "pi" }, fixture, () => {}), /step=admit E2E_PI/);
  await assert.rejects(qualification(processes, { format: "installed-host", host: "shell" }, fixture, () => {}), /step=admit native host/);
});

// The crate whose library tests each exact-selection binary runs.
const LIBTEST_CRATES = { E2E_NATIVE_RUNTIME_TEST_BINARY: "native-runtime" };

function moduleBody(text, name) {
  const start = text.search(new RegExp(String.raw`\bmod ${name}\s*\{`));
  if (start < 0) return undefined;
  let depth = 0;
  for (let index = text.indexOf("{", start); index < text.length; index++) {
    if (text[index] === "{") depth++;
    else if (text[index] === "}" && --depth === 0) return text.slice(start, index + 1);
  }
  return undefined;
}

// True when `path` (module::...::name, from the crate root) is a #[test] function
// in its own module: a file module or an inline `mod` block, never a helper.
async function declaresTest(source, path) {
  const segments = path.split("::"), name = segments.pop();
  const attribute = new RegExp(String.raw`#\[(?:[a-z_]+::)?test(?:\([^)]*\))?\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([a-z]+\))?\s+)?(?:async\s+)?fn ${name}\s*\(`);
  for (let split = segments.length; split >= 0; split--) {
    const outer = segments.slice(0, split), inline = segments.slice(split);
    const files = outer.length === 0 ? [join(source, "lib.rs")]
      : [join(source, ...outer) + ".rs", join(source, ...outer, "mod.rs")];
    for (const file of files) {
      let text = await readFile(file, "utf8").catch(() => undefined);
      for (const module of inline) text = text === undefined ? undefined : moduleBody(text, module);
      if (text !== undefined && attribute.test(text)) return true;
    }
  }
  return false;
}

test("an exact test name resolves only to a test in its own module", async () => {
  const source = await mkdtemp(join(tmpdir(), "exact-tests-"));
  await mkdir(join(source, "execution"));
  await writeFile(join(source, "lib.rs"), "mod execution;\nmod inline;\n");
  await writeFile(join(source, "execution", "tests.rs"),
    "#[cfg(unix)]\n#[test]\nfn real() {}\nfn helper() {}\n");
  await writeFile(join(source, "inline.rs"), "fn outside() {}\nmod tests {\n    #[tokio::test]\n    async fn inner() {}\n}\n");
  assert.equal(await declaresTest(source, "execution::tests::real"), true);
  assert.equal(await declaresTest(source, "inline::tests::inner"), true);
  for (const path of ["execution::tests::helper", "wrong::tests::real", "execution::real",
    "inline::tests::outside", "execution::tests::absent"])
    assert.equal(await declaresTest(source, path), false, path);
});

test("every exact libtest qualification names tests that exist and requires all of them", async () => {
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const cases = join(root, "scripts/graphcoder-e2e/cases");
  let checked = 0;
  for (const file of await readdir(cases)) {
    if (!file.endsWith(".json")) continue;
    for (const input of JSON.parse(await readFile(join(cases, file), "utf8")).inputs ?? []) {
      if (input.format !== "libtest" || !input.args?.includes("--exact")) continue;
      const crate = LIBTEST_CRATES[input.binary_env];
      assert.ok(crate, `${file}: name the crate ${input.binary_env} runs in LIBTEST_CRATES`);
      const names = input.args.filter(argument => !argument.startsWith("--"));
      assert.equal(input.minimum_tests, names.length, `${file}: an exact selection requires every named test`);
      for (const name of names)
        assert.ok(await declaresTest(join(root, "rust/crates", crate, "src"), name),
          `${file}: ${name} is not a test in the tree`);
      checked++;
    }
  }
  assert.ok(checked > 0, "no exact libtest qualification was checked");
});
