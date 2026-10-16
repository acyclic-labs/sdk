import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { fetchLocked } from "./cargo-fetch.mjs";

const scripts = dirname(fileURLToPath(import.meta.url));

function assertOfflineFetches(source, name) {
  let count = 0;
  for (const call of source.matchAll(/\b(execFileSync|spawnSync|run)\("cargo",\s*\[([\s\S]*?)\]([\s\S]*?);/gu)) {
    if (!call[2].includes('"--offline"')) continue;
    const preceding = source.slice(0, call.index);
    const fetch = /fetchLocked\((\w+)\);\s*(?:(?:const|let)\s+\w+\s*=\s*)?(?:JSON\.parse\()?$/u.exec(preceding);
    assert.ok(fetch, `${name}: offline cargo must immediately follow fetchLocked`);
    const root = fetch[1];
    assert.match(source, /import \{ fetchLocked \} from "\.\/cargo-fetch\.mjs";/u);
    assert.ok(!call[2].includes('"--manifest-path"'), `${name}: explicit manifests need a matching fetch`);
    if (call[1] === "run") {
      assert.match(source, new RegExp(`function run\\(command, args, cwd = ${root}\\)`));
      assert.match(source, /spawnSync\(command, args, \{ cwd,/u);
      assert.match(call[3], /^\s*\)/u, `${name}: run must use its fetched default cwd`);
    } else {
      assert.match(call[3], new RegExp(`cwd: ${root}\\b`), `${name}: fetch must use the offline manifest's cwd`);
    }
    count++;
  }
  return count;
}

test("locked fetch populates the same manifest even under inherited offline mode", () => {
  const root = resolve(scripts, "..");
  const previous = process.env.CARGO_NET_OFFLINE;
  try {
    process.env.CARGO_NET_OFFLINE = "true";
    let calls = 0;
    fetchLocked(root, (command, args, options) => {
      calls++;
      assert.equal(command, "cargo");
      assert.deepEqual(args, ["fetch", "--locked", "--manifest-path", join(root, "Cargo.toml")]);
      assert.equal(options.cwd, root);
      assert.equal(options.stdio, "inherit");
      assert.deepEqual(options.env, { ...process.env, CARGO_NET_OFFLINE: "false" });
    });
    assert.equal(calls, 1);
    assert.equal(process.env.CARGO_NET_OFFLINE, "true");
  } finally {
    if (previous === undefined) delete process.env.CARGO_NET_OFFLINE;
    else process.env.CARGO_NET_OFFLINE = previous;
  }
});

test("failed fetch refuses the offline consumer", () => {
  const failure = new Error("registry unavailable");
  let consumed = false;
  assert.throws(() => {
    fetchLocked(scripts, () => { throw failure; });
    consumed = true;
  }, error => error === failure);
  assert.equal(consumed, false);
});

test("every production JavaScript offline cargo spawn fetches its own manifest first", () => {
  let count = 0;
  for (const name of readdirSync(scripts)) {
    if (!name.endsWith(".mjs") || name.endsWith(".test.mjs") || name.startsWith("test-")) continue;
    count += assertOfflineFetches(readFileSync(join(scripts, name), "utf8"), name);
  }
  assert.ok(count >= 8, `expected all eight codegen calls, found ${count}`);
});

test("offline spawn guard rejects missing, late and different-manifest fetches", () => {
  const imported = 'import { fetchLocked } from "./cargo-fetch.mjs";\n';
  const offline = 'spawnSync("cargo", ["run", "--offline", "--locked"], { cwd: root });';
  for (const body of [offline, `${offline}\nfetchLocked(root);`, `fetchLocked(other);\n${offline}`]) {
    assert.throws(() => assertOfflineFetches(imported + body, "fixture"));
  }
  assert.equal(assertOfflineFetches(imported + `fetchLocked(root);\n${offline}`, "fixture"), 1);
});
