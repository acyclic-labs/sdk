import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { filesByEdition, parseManifest, qualifyRustfmt } from "./qualify-rustfmt.mjs";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const clean = "pub fn f() -> u8 {\n    1\n}\n";
const drift = "pub fn f()->u8{1}\n";

// A root workspace with one member, plus the shapes `cargo fmt --all` never
// visits: a nested standalone 2021 crate (where `gen` is an identifier, which
// the 2024 parser rejects), a module declared only through a macro, an
// out-of-tree fixture, and a vendored crate that must stay untouched. A plain
// child module covers the `skip_children` batching.
const tree = {
  "Cargo.toml": "[workspace]\nmembers = [\"member\"]\nexclude = [\"rust/vendor/dep\"]\n\n[workspace.package]\nedition = \"2024\" # pinned\n",
  "member/Cargo.toml": "[package]\nname = \"member\"\nedition.workspace = true\n",
  "member/src/lib.rs": "mod plain;\nmacro_rules! hidden {\n    () => {\n        mod hidden;\n    };\n}\nhidden!();\n",
  "member/src/hidden.rs": clean,
  "member/src/plain.rs": clean,
  "member/fuzz/Cargo.toml": "[package]\nname = \"member-fuzz\"\nedition = \"2021\"\n\n[workspace]\n",
  "member/fuzz/fuzz_targets/target.rs": "pub fn f() -> u8 {\n    let gen = 1;\n    gen\n}\n",
  "member/tests/fixtures/source.rs": clean,
  "rust/vendor/dep/Cargo.toml": "[package]\nname = \"dep\"\n",
  "rust/vendor/dep/src/lib.rs": drift,
};

function fixture(overrides = {}) {
  const root = mkdtempSync(join(tmpdir(), "qualify-rustfmt-"));
  for (const name of ["rust-toolchain.toml", "rustfmt.toml"]) copyFileSync(join(repo, name), join(root, name));
  for (const [path, text] of Object.entries({ ...tree, ...overrides })) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  }
  execFileSync("git", ["init", "-q"], { cwd: root });
  execFileSync("git", ["add", "-A"], { cwd: root });
  return root;
}

test("manifests resolve explicit, inherited, default and virtual editions", () => {
  assert.deepEqual(parseManifest(tree["Cargo.toml"]), { workspace: "2024", edition: null });
  assert.deepEqual(parseManifest(tree["member/Cargo.toml"]), { workspace: null, edition: "inherit" });
  assert.deepEqual(parseManifest("[package]\nedition = { workspace = true }\n"), { workspace: null, edition: "inherit" });
  assert.deepEqual(parseManifest(tree["member/fuzz/Cargo.toml"]), { workspace: "2015", edition: "2021" });
  assert.deepEqual(parseManifest("[package]\nname = \"x\"\n[dependencies]\nedition = \"2018\"\n"), { workspace: null, edition: "2015" });
});

test("every tracked non-vendored Rust file is checked under its crate's edition", () => {
  const root = fixture();
  try {
    assert.deepEqual(Object.fromEntries(filesByEdition(root)), {
      2021: ["member/fuzz/fuzz_targets/target.rs"],
      2024: ["member/src/hidden.rs", "member/src/lib.rs", "member/src/plain.rs", "member/tests/fixtures/source.rs"],
    });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("the gate passes a clean tree and fails drift planted in any crate shape", async () => {
  const root = fixture();
  try { assert.deepEqual(await qualifyRustfmt(root), { status: 0, output: "" }); }
  finally { rmSync(root, { recursive: true, force: true }); }
  const planted = {
    "member/src/hidden.rs": drift,
    "member/src/plain.rs": drift,
    "member/fuzz/fuzz_targets/target.rs": "pub fn f()->u8{let gen=1;gen}\n",
    "member/tests/fixtures/source.rs": drift,
    "member/src/lib.rs": "mod plain;\nhidden ! ();\n",
  };
  for (const [path, text] of Object.entries(planted)) {
    const root = fixture({ [path]: text });
    try {
      const { status, output } = await qualifyRustfmt(root);
      assert.equal(status, 1, path);
      assert.match(output.replaceAll("\\", "/"), new RegExp(`${path}:\\d+:`), path);
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});
