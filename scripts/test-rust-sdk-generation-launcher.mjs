import assert from "node:assert/strict";
import test from "node:test";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import { generationInvocation } from "./rust-sdk-generation.mjs";

const options = {
  repositoryRoot: resolve("/frozen/sdk"), callerDirectory: resolve("/caller"),
  temporaryRoot: resolve("/temporary"), environment: {},
};
const value = (plan, option) => plan.args[plan.args.lastIndexOf(option) + 1];
const contains = (parent, child) => {
  const path = relative(parent, child);
  return path === "" || (!path.startsWith("..") && !isAbsolute(path));
};

test("default artifacts and compiler cache are disjoint from the frozen source", () => {
  const plan = generationInvocation("generate", [], options);
  const source = value(plan, "--source-root");
  for (const destination of [value(plan, "--output"), plan.options.env.CARGO_TARGET_DIR]) {
    assert.equal(contains(source, destination), false);
    assert.equal(contains(destination, source), false);
  }
  assert.equal(plan.options.cwd, source);
  assert.equal(value(plan, "--manifest-path"), join(source, "rust/crates/sdk-generation/Cargo.toml"));
  assert.equal(value(plan, "--bin"), "sdk-generation");
});

test("generate and drift share defaults while independent checkouts have separate artifacts", () => {
  const generated = generationInvocation("generate", [], options);
  const drift = generationInvocation("drift", [], options);
  const other = generationInvocation("generate", ["--source-root", "other"], options);
  assert.equal(value(generated, "--output"), value(drift, "--output"));
  assert.notEqual(value(generated, "--output"), value(other, "--output"));
});

test("explicit caller paths and pinned runtime/cache are preserved", () => {
  const environment = { SDK_CARGO: "pinned-cargo", CARGO_TARGET_DIR: "owned-cache", KEEP: "value" };
  const plan = generationInvocation("check", ["--source-root", "snapshot", "--output", "packages"], { ...options, environment });
  assert.equal(plan.program, "pinned-cargo");
  assert.equal(value(plan, "--source-root"), resolve(options.callerDirectory, "snapshot"));
  assert.equal(value(plan, "--output"), resolve(options.callerDirectory, "packages"));
  assert.equal(plan.options.env.CARGO_TARGET_DIR, resolve(options.callerDirectory, "owned-cache"));
  assert.equal(plan.options.env.KEEP, "value");
  assert.equal(environment.CARGO_TARGET_DIR, "owned-cache");
});

test("explicit Cargo target directories cannot write inside the frozen source", () => {
  assert.throws(
    () => generationInvocation(
      "generate",
      ["--source-root", "/frozen/sdk"],
      { ...options, environment: { CARGO_TARGET_DIR: "/frozen/sdk/target" } },
    ),
    /Cargo target directory must be outside the Rust source root/,
  );
});

test("missing path values fail before a compiler or generator starts", () => {
  for (const args of [["--source-root"], ["--output"], ["--source-root", "--output", "x"]]) {
    assert.throws(() => generationInvocation("generate", args, options), /requires a path/);
  }
});

test("explicit output cannot reintroduce generated files into the frozen source", () => {
  assert.throws(
    () => generationInvocation("generate", ["--source-root", "/frozen/sdk", "--output", "/frozen/sdk/target/sdk-generation"], options),
    /generation output must be outside the Rust source root/,
  );
});

test("configured Rust output defaults also apply through the thin launcher", () => {
  const configured = { ...options, environment: { ACYCLIC_SDK_GENERATION_OUTPUT: "retained-artifacts" } };
  const implicit = generationInvocation("generate", [], configured);
  assert.equal(value(implicit, "--output"), resolve(options.callerDirectory, "retained-artifacts"));
  const explicit = generationInvocation("generate", ["--output", "explicit"], configured);
  assert.equal(value(explicit, "--output"), resolve(options.callerDirectory, "explicit"));
});

test("SDK_BUILD_ROOT owns output and source-keyed Cargo/rustdoc caches", () => {
  const configured = {
    ...options,
    temporaryRoot: undefined,
    environment: { SDK_BUILD_ROOT: "/configured/sdk-build" },
  };
  const first = generationInvocation("generate", [], configured);
  const second = generationInvocation("drift", ["--source-root", "other"], configured);
  const buildRoot = resolve(options.callerDirectory, "/configured/sdk-build");
  const generationRoot = join(buildRoot, "acyclic-sdk-generation");
  assert.equal(first.options.env.SDK_BUILD_ROOT, buildRoot);
  assert.match(first.options.env.CARGO_TARGET_DIR, new RegExp(`${generationRoot.replaceAll("\\", "\\\\")}\\\\[0-9a-f]{16}\\\\cargo-target$`));
  assert.match(first.options.env.SDK_DOCS_RUSTDOC_CACHE_DIR, new RegExp(`${generationRoot.replaceAll("\\", "\\\\")}\\\\[0-9a-f]{16}\\\\rustdoc-cache$`));
  assert.notEqual(first.options.env.CARGO_TARGET_DIR, second.options.env.CARGO_TARGET_DIR);
  assert.notEqual(first.options.env.SDK_DOCS_RUSTDOC_CACHE_DIR, second.options.env.SDK_DOCS_RUSTDOC_CACHE_DIR);
  assert.equal(first.options.env.CARGO_TARGET_DIR.startsWith(first.options.env.SDK_BUILD_ROOT), true);
});

test("explicit target and docs cache remain caller-owned under SDK_BUILD_ROOT", () => {
  const environment = {
    SDK_BUILD_ROOT: "/configured/sdk-build",
    CARGO_TARGET_DIR: "retained/cargo-target",
    SDK_DOCS_RUSTDOC_CACHE_DIR: "retained/rustdoc-cache",
  };
  const plan = generationInvocation("check", [], { ...options, temporaryRoot: undefined, environment });
  assert.equal(plan.options.env.CARGO_TARGET_DIR, resolve(options.callerDirectory, "retained/cargo-target"));
  assert.equal(plan.options.env.SDK_DOCS_RUSTDOC_CACHE_DIR, resolve(options.callerDirectory, "retained/rustdoc-cache"));
  assert.equal(plan.options.env.SDK_BUILD_ROOT, resolve(options.callerDirectory, "/configured/sdk-build"));
});

test("explicit rustdoc cache directories cannot write inside the frozen source", () => {
  assert.throws(
    () => generationInvocation(
      "generate",
      ["--source-root", "/frozen/sdk"],
      { ...options, environment: { SDK_DOCS_RUSTDOC_CACHE_DIR: "/frozen/sdk/rustdoc-cache" } },
    ),
    /Rustdoc cache directory must be outside the Rust source root/,
  );
});

test("contract launcher keeps its PowerShell and Node entrypoints source-independent", () => {
  const powershell = readFileSync(new URL("./run-rust-contract-generator.ps1", import.meta.url), "utf8");
  const node = readFileSync(new URL("./run-rust-contract-generator.mjs", import.meta.url), "utf8");
  assert.match(powershell, /ACYCLIC_SDK_WORK_ROOT/);
  assert.match(powershell, /Contract output must be outside the Rust source root/);
  assert.doesNotMatch(powershell, /C:\\Users\\varun\\\.codex\\worktrees/);
  assert.match(node, /--bin/, "Node launcher must invoke the Rust-owned binary");
  assert.match(node, /rust\/crates\/sdk-typescript\/Cargo\.toml/, "the launcher must use the Rust TypeScript generator crate");
  assert.match(node, /sdk-contracts/, "the launcher must invoke the Rust contract binary");
  assert.doesNotMatch(node, /typescript\/packages|generated-client\.ts/, "the launcher must not encode a TypeScript contract path");
  assert.match(node, /CARGO_TARGET_DIR/, "Node launcher must isolate Cargo output");
  assert.match(node, /invokedAsCli/, "PowerShell launcher must have a callable Node entrypoint");
  const filesystemPackage = readFileSync(new URL("../typescript/packages/filesystem/package.json", import.meta.url), "utf8");
  assert.match(filesystemPackage, /"build:proto"\s*:\s*"cd \.\.\/\.\.\/\.\. && bun run generate"/);
  assert.doesNotMatch(filesystemPackage, /buf (?:format|lint)/, "package hooks must not treat the legacy proto tree as a generation authority");
});

test("Kotlin producer stages Maven output outside the Rust source checkout when the JVM profile is present", () => {
  const adapterPath = new URL("../jvm/kotlin-producer-adapter.ps1", import.meta.url);
  if (!existsSync(adapterPath)) {
    // The primary Rust/docs source profile intentionally omits the second PR's
    // language trees; keep this profile-scoped test explicit rather than
    // importing a source file that is outside this checkout's contract.
    assert.equal(existsSync(adapterPath), false);
    return;
  }
  const adapter = readFileSync(adapterPath, "utf8");
  assert.match(adapter, /workspaceJvm/);
  assert.match(adapter, /maven\.repo\.local/);
  assert.match(adapter, /RuntimeInformation/);
  assert.match(adapter, /Copy-Item/);
  assert.doesNotMatch(adapter, /robocopy/i, "Kotlin staging must work under PowerShell Core on Unix");
  assert.doesNotMatch(adapter, /Join-Path \$root ['"]jvm[\\/]target/);
});
