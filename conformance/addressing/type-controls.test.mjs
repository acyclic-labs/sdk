import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

test("package-local build graphs schedule Auth before every identity facade", () => {
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const compiler = join(root, "node_modules/typescript/bin/tsc");
  const packages = ["actors", "stream", "objects", "filesystem", "machines", "workers", "sdk"];
  const requiresAuth = config => assert.ok(
    config.references?.some(reference => reference.path === "../auth"),
    "an identity facade must reference its Auth declaration producer",
  );
  for (const name of packages) {
    const configPath = join(root, "typescript/packages", name, "tsconfig.json");
    const config = JSON.parse(readFileSync(configPath, "utf8"));
    requiresAuth(config);
    assert.throws(() => requiresAuth({ ...config,
      references: config.references.filter(reference => reference.path !== "../auth"),
    }), /must reference/);
    const result = spawnSync(process.execPath, [compiler, "-b", configPath, "--dry", "--verbose"], {
      cwd: root, encoding: "utf8",
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const auth = result.stdout.indexOf("typescript/packages/auth/tsconfig.json");
    const consumer = result.stdout.indexOf(`typescript/packages/${name}/tsconfig.json`);
    assert.ok(auth >= 0 && consumer > auth, result.stdout);
  }
});

test("all public identity bindings accept byte fields and reject text and mutable field shapes", () => {
  // The umbrella SDK declares all eight dependencies. Put its temporary consumer
  // inside that package so isolated installs resolve the declared graph; the
  // repository root deliberately has no SDK runtime dependencies to hoist.
  const directory = mkdtempSync(fileURLToPath(new URL(
    "../../typescript/packages/sdk/test/.type-consumer-", import.meta.url,
  )));
  const packages = ["auth", "actors", "stream", "objects", "fs", "machines", "workers", "sdk"];
  const imports = packages.map((name, index) =>
    `import { ResourceIdentity as Identity${index}, ActorReference as Actor${index} } from "@acyclic-labs/${name}/addressing";`).join("\n");
  const positive = packages.map((_, index) => `{
    const view = Identity${index}.fromFields({ account: new Uint8Array([97]), id: new Uint8Array([98]) });
    const fields = Identity${index}.decode(view.encode()).toFields();
    const account: Uint8Array = fields.account;
    const id: Uint8Array = fields.id;
    const equal: boolean = view.equals(new Identity${index}(account, id));
    const host: string = view.hostname("example.com");
    const actor = new Actor${index}(account, new TextEncoder().encode("ClassOne"), new TextEncoder().encode("same"));
    const restored = Actor${index}.decode(actor.encode());
    const namespace: Uint8Array = restored.toFields().namespace;
    const sameActor: boolean = restored.equals(actor);
  }`).join("\n");
  const negative = packages.map((_, index) => `{
    new Identity${index}("account", "id");
    const fields = new Identity${index}(new Uint8Array([1]), new Uint8Array([2])).toFields();
    fields.id = new Uint8Array([3]);
    new Actor${index}(new Uint8Array([1]), "ClassOne", new Uint8Array([2]));
    const actorFields = Actor${index}.fromLegacyFields(new Uint8Array([1]), undefined, "same").toFields();
    actorFields.namespace = new Uint8Array([3]);
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
    assert.equal((rejected.stdout.match(/error TS2345:/g) ?? []).length, packages.length * 2, rejected.stdout);
    assert.equal((rejected.stdout.match(/error TS2540:/g) ?? []).length, packages.length * 2, rejected.stdout);
    assert.equal((rejected.stdout.match(/error TS\d+:/g) ?? []).length, packages.length * 4, rejected.stdout);
  } finally {
    rmSync(directory, { recursive: true });
  }
});
