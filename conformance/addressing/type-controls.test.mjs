import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

test("all public identity bindings accept byte fields and reject text and mutable field shapes", () => {
  // The umbrella SDK declares all eight dependencies. Put its temporary consumer
  // inside that package so isolated installs resolve the declared graph; the
  // repository root deliberately has no SDK runtime dependencies to hoist.
  const directory = mkdtempSync(fileURLToPath(new URL(
    "../../typescript/packages/sdk/test/.type-consumer-", import.meta.url,
  )));
  const packages = ["auth", "actors", "stream", "objects", "fs", "machines", "workers", "sdk"];
  const imports = packages.map((name, index) =>
    `import { ResourceIdentity as Identity${index} } from "@acyclic-labs/${name}/addressing";`).join("\n");
  const positive = packages.map((_, index) => `{
    const view = Identity${index}.fromFields({ account: new Uint8Array([97]), id: new Uint8Array([98]) });
    const fields = Identity${index}.decode(view.encode()).toFields();
    const account: Uint8Array = fields.account;
    const id: Uint8Array = fields.id;
    const equal: boolean = view.equals(new Identity${index}(account, id));
    const host: string = view.hostname("example.com");
  }`).join("\n");
  const negative = packages.map((_, index) => `{
    new Identity${index}("account", "id");
    const fields = new Identity${index}(new Uint8Array([1]), new Uint8Array([2])).toFields();
    fields.id = new Uint8Array([3]);
  }`).join("\n");
  const compiler = fileURLToPath(new URL("../../node_modules/typescript/bin/tsc", import.meta.url));
  const compile = source => {
    const path = join(directory, "consumer.mts");
    writeFileSync(path, imports + "\n" + source);
    const result = spawnSync(process.execPath, [compiler, "--ignoreConfig", "--noEmit", "--strict",
      "--skipLibCheck", "--target", "ES2022", "--module", "NodeNext", "--moduleResolution", "NodeNext",
      "--pretty", "false", path], { encoding: "utf8" });
    assert.ifError(result.error);
    return result;
  };
  try {
    const accepted = compile(positive);
    assert.equal(accepted.status, 0, accepted.stdout + accepted.stderr);
    const rejected = compile(negative);
    assert.notEqual(rejected.status, 0, "invalid consumers were accepted");
    assert.equal((rejected.stdout.match(/error TS2345:/g) ?? []).length, packages.length, rejected.stdout);
    assert.equal((rejected.stdout.match(/error TS2540:/g) ?? []).length, packages.length, rejected.stdout);
    assert.equal((rejected.stdout.match(/error TS\d+:/g) ?? []).length, packages.length * 2, rejected.stdout);
  } finally {
    rmSync(directory, { recursive: true });
  }
});
