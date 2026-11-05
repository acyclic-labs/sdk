import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { credentialFixturePaths as fixtures } from "./check-boundaries.mjs";

const root = resolve(import.meta.dirname, "..");
const checker = readFileSync(join(root, "scripts/check-boundaries.mjs"));

test("Cargo executable consumers use the runtime-aware locator", t => {
  const path = "rust/crates/harness-mock-model/tests/http.rs";
  const source = readFileSync(join(root, path), "utf8");
  const files = { [path]: source,
    "rust/crates/test-support/src/fixtures.rs": readFileSync(join(root, "rust/crates/test-support/src/fixtures.rs"), "utf8") };
  assert.equal(scan(t, files).status, 0);
  const restored = source.replace('acyclic_test_support::cargo_binary!("harness-mock-model")',
    'env!("CARGO_BIN_EXE_harness-mock-model")');
  assert.notEqual(restored, source);
  const result = scan(t, { ...files, [path]: restored });
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /use acyclic_test_support::cargo_binary!/);
});

function scan(t, files, untracked = {}) {
  mkdirSync(join(root, ".tools"), { recursive: true });
  const directory = mkdtempSync(join(root, ".tools/boundary-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const write = entries => {
    for (const [path, content] of Object.entries(entries)) {
      mkdirSync(dirname(join(directory, path)), { recursive: true });
      writeFileSync(join(directory, path), content);
    }
  };
  write({ "scripts/check-boundaries.mjs": checker, "scripts/process-capture.mjs": readFileSync(join(root, "scripts/process-capture.mjs")), ...files });
  execFileSync("git", ["init", "--quiet", directory]);
  execFileSync("git", ["add", "."], { cwd: directory });
  write(untracked);
  return spawnSync(process.execPath, ["scripts/check-boundaries.mjs"], { cwd: directory, encoding: "utf8" });
}

test("tracked public types and recorded fake-server fixtures pass on LF and CRLF checkouts", t => {
  for (const newline of ["\n", "\r\n"]) {
    const files = Object.fromEntries(fixtures.map(path =>
      [path, readFileSync(join(root, path), "utf8").replaceAll("\r\n", "\n").replaceAll("\n", newline)]));
    files["typescript/packages/stream/src/public-input.ts"] = "export interface Input { github_" + "token: string; }";
    const result = scan(t, files);
    assert.equal(result.status, 0, result.stderr);
  }
});

test("credential admission binds both fixture paths and complete contents", t => {
  const files = {};
  for (const [index, path] of fixtures.entries()) {
    const content = readFileSync(join(root, path), "utf8");
    files[path] = content + "\nchanged\n";
    files[`copied-${index}.mjs`] = content;
  }
  const result = scan(t, files);
  assert.equal(result.status, 1);
  for (const path of Object.keys(files)) {
    assert.ok(result.stderr.includes(`${path}: credential outside a recorded fake-server fixture`), result.stderr);
  }
});

test("literal source credentials, plaintext configuration and private implementation leaks reject", t => {
  const cases = [
    ["consumer.ts", 'const input = { github_' + 'token: "unrecorded-value" };'],
    ["settings.env", "aws_secret_" + "access_key=unrecorded-value"],
    ["identity.pem", "-----BEGIN " + "PRIVATE KEY-----"],
    ["example.rs", "use acyclic_" + "internal::client;"],
    ["proto/private/admin.proto", 'syntax = "proto3";'],
    ["rust/crates/machines/proto/machines/v1/leak.proto", "package vmm." + "fleet.v1;"],
  ];
  const result = scan(t, Object.fromEntries(cases));
  assert.equal(result.status, 1, result.stderr);
  for (const [path] of cases) {
    assert.ok(result.stderr.includes(path), result.stderr);
  }
});

test("backticks, quoted dollar/braces and authorization literals reject", t => {
  const values = ['`unrecorded-secret`', '"unrecorded$secret"', '"unrecorded{secret}"', '"${secret}"', '`unrecorded\\${secret}`'];
  const files = Object.fromEntries(values.map((value, index) => [`consumer-${index}.ts`, "const github_" + "token = " + value + ";"]));
  files["consumer.mjs"] = 'const headers = { author' + 'ization: "Bearer unrecorded" };';
  const result = scan(t, files);
  assert.equal(result.status, 1, result.stderr);
  for (const path of Object.keys(files)) {
    assert.ok(result.stderr.includes(`${path}: credential outside a recorded fake-server fixture`), result.stderr);
  }
});

test("credential literals nested inside interpolated headers reject", t => {
  const content = "const headers = { author" +
    'ization: `Bearer ${getToken({ github_' + 'token: "unrecorded-secret" })}` };';
  const result = scan(t, { "consumer.ts": content });
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /credential outside a recorded fake-server fixture/);
});

test("headers computed from runtime credentials remain valid", t => {
  const result = scan(t, { "consumer.ts": "const headers = { author" + 'ization: `Bearer ${options.token}` };' });
  assert.equal(result.status, 0, result.stderr);
});

test("the source guard ignores untracked local reports and checks tracked dist files", t => {
  const content = "use acyclic_" + "internal::client;";
  assert.equal(scan(t, {}, { ".predict.md": content, ".tools/report.log": content }).status, 0);
  const result = scan(t, { "dist/consumer.rs": content });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /dist\/consumer.rs/);
});
