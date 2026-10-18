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
  assert.deepEqual(parseManifest(tree["member/fuzz/Cargo.toml"]), { workspace: false, edition: "2021" });
  assert.deepEqual(parseManifest("[package]\nname = \"x\"\n[dependencies]\nedition = \"2018\"\n"), { workspace: null, edition: "2015" });
});

test("TOML edition spellings have the same meaning and malformed manifests fail closed", () => {
  for (const edition of ["2015", "2018", "2021", "2024"]) {
    for (const value of [`'${edition}'`, `"${edition}"`, `'''${edition}'''`, `"""${edition}"""`]) {
      for (const header of ["[package]", "[ 'package' ]", '[ "package" ]']) {
        assert.deepEqual(parseManifest(`${header}\n'edition' = ${value} # comment\n`), { workspace: null, edition });
      }
    }
  }
  assert.deepEqual(parseManifest('package.edition.workspace = true\nworkspace.package.edition = "2021"\n'), { workspace: "2021", edition: "inherit" });
  assert.throws(() => parseManifest('[package]\nedition = "2021"\nedition = "2024"\n'));
});

test("an explicit package workspace owns the inherited edition", () => {
  const root = fixture({
    "member/Cargo.toml": '[package]\nname = "member"\nedition.workspace = true\nworkspace = "../owner"\n',
    "owner/Cargo.toml": '[workspace]\nmembers = ["../member"]\n[workspace.package]\nedition = "2021"\n',
  });
  try { assert.equal(filesByEdition(root).has("2024"), false); }
  finally { rmSync(root, { recursive: true, force: true }); }
});

test("workspace inheritance fails closed at a workspace without an edition", () => {
  const root = fixture({ "Cargo.toml": '[workspace]\nmembers = ["member"]\n' });
  try { assert.throws(() => filesByEdition(root), /no workspace.package.edition/); }
  finally { rmSync(root, { recursive: true, force: true }); }
});

test("grouping visits each file once without copying prior groups", () => {
  const root = fixture();
  const iterate = Array.prototype[Symbol.iterator];
  let visits = 0;
  try {
    Array.prototype[Symbol.iterator] = function () {
      const iterator = iterate.call(this);
      return {
        next() {
          const item = iterator.next();
          if (item.value === "member/src/hidden.rs") visits++;
          return item;
        },
        [Symbol.iterator]() { return this; },
      };
    };
    filesByEdition(root);
  } finally {
    Array.prototype[Symbol.iterator] = iterate;
    rmSync(root, { recursive: true, force: true });
  }
  assert.equal(visits, 1);
});

test("legal TOML strings cannot hide or spoof the package edition", () => {
  for (const manifest of [
    "[package]\nname = 'member-fuzz'\nedition = '2021'\n[workspace]\n",
    '[package]\nname = "member-fuzz"\nedition = "2021"\ndescription = """\n[package]\nedition = "2024"\n"""\n[workspace]\n',
  ]) {
    const root = fixture({ "member/fuzz/Cargo.toml": manifest });
    try {
      assert.deepEqual(filesByEdition(root).get("2021"), ["member/fuzz/fuzz_targets/target.rs"]);
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
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
