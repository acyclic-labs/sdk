import assert from "node:assert/strict";
import { isAbsolute } from "node:path";

export function qualified(result, format, minimum) {
  assert.equal(result.code, 0, result.stderr);
  assert.ok(Number.isSafeInteger(minimum) && minimum > 0, "qualification needs a positive case count");
  const text = result.stdout + "\n" + result.stderr;
  if (format === "libtest") {
    const summaries = [...text.matchAll(/^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;(?: \d+ measured; \d+ filtered out; finished in [^\r\n]+)?\r?$/gm)];
    assert.equal(summaries.length, 1, "qualification needs exactly one libtest summary");
    assert.equal(Number(summaries[0][2]), 0, "qualification had failed tests");
    assert.equal(Number(summaries[0][3]), 0, "qualification had ignored tests");
    assert.ok(Number(summaries[0][1]) >= minimum, "qualification ran zero or too few tests");
  } else if (format === "bun") {
    const passed = [...text.matchAll(/^\s*(\d+) pass\s*$/gm)];
    const failed = [...text.matchAll(/^\s*(\d+) fail\s*$/gm)];
    assert.equal(passed.length, 1, "qualification needs exactly one Bun pass summary");
    assert.equal(failed.length, 1, "qualification needs exactly one Bun fail summary");
    assert.ok(Number(passed[0][1]) >= minimum, "qualification ran zero or too few Bun tests");
    assert.equal(Number(failed[0][1]), 0, "qualification had failed Bun tests");
    assert.doesNotMatch(text, /^\s*[1-9]\d* skip\s*$/m, "qualification had skipped tests");
  } else assert.fail(`unknown qualification format ${format}`);
}

export async function qualification(processes, input, fixture, capture) {
  const binary = fixture.env[input.binary_env];
  assert.ok(binary && isAbsolute(binary), `${input.binary_env} must name a prebuilt absolute executable`);
  const env = { ...fixture.env, RUST_TEST_THREADS: "1" };
  if (input.fixture_binary_env) {
    assert.ok(isAbsolute(env[input.fixture_binary_env] ?? ""), `${input.fixture_binary_env} must name the matching prebuilt plugin executable`);
    env.ACYCLIC_TEST_BINARY = env[input.fixture_binary_env];
    env.ACYCLIC_BINARY = env[input.fixture_binary_env];
  }
  const result = await processes.runOwned(binary, input.args, { cwd: fixture.workspace, env });
  capture(result);
  qualified(result, input.format, input.minimum_tests);
}
