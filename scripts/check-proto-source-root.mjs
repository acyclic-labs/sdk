import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const parent = resolve(process.env.SDK_TEMP_DIR ?? tmpdir());
const temporary = mkdtempSync(join(parent, "sdk-proto-source-"));
const environment = {
  ...process.env,
  CARGO_INCREMENTAL: "0",
  CARGO_TARGET_DIR: resolve(process.env.CARGO_TARGET_DIR ?? join(root, "target")),
};
delete environment.ACYCLIC_BUILD_SOURCE_ROOT;
const compatibility = join(root, "compatibility").replaceAll("\\", "/");
const oldTime = new Date("2000-01-01T00:00:00Z");
function cargo(directory, args) {
  const result = spawnSync("cargo", args, { cwd: directory, env: environment, encoding: "utf8", windowsHide: true });
  if (result.error) throw result.error;
  return result;
}
function requireSuccess(result, step) {
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    process.stderr.write(result.stdout ?? "");
    throw new Error(`${step} failed: ${result.status ?? result.signal}`);
  }
}
try {
  for (const tracked of [false, true]) {
    const name = `sdk-proto-source-${tracked ? "tracked" : "unbound"}`;
    const directories = ["first", "second"].map(side => join(temporary, name, side));
    for (const [index, directory] of directories.entries()) {
      mkdirSync(join(directory, "crate", "proto"), { recursive: true });
      const field = index === 0 ? "first" : "second";
      const value = index + 11;
      const files = {
        "Cargo.toml": `[workspace]\nresolver = "3"\nmembers = ["crate"]\n[workspace.dependencies]\nprost = "=0.14.4"\nacyclic-proto-build = { path = "${compatibility}" }\n`,
        "crate/Cargo.toml": `[package]\nname = "${name}"\nversion = "0.0.0"\nedition = "2024"\n[dependencies]\nprost.workspace = true\n[build-dependencies]\nacyclic-proto-build.workspace = true\n[[bin]]\nname = "${name}"\npath = "main.rs"\n`,
        "crate/build.rs": `fn main() -> Result<(), Box<dyn std::error::Error>> {\nlet compiler = acyclic_proto_build::Compiler::new()?;\ncompiler.compile(compiler.config(), acyclic_proto_build::services(false), &["proto/source.proto"], "source.bin")\n}\n`,
        "crate/proto/source.proto": `syntax = "proto3"; package relocation; message Marker { uint32 ${field} = ${index + 1}; }\n`,
        "crate/main.rs": `include!(concat!(env!("OUT_DIR"), "/relocation.rs"));\nfn main() { let marker = Marker { ${field}: ${value} }; println!("{}", marker.${field}); }\n`,
      };
      mkdirSync(join(directory, ".cargo"));
      const configuration = join(directory, ".cargo/config.toml");
      copyFileSync(join(root, ".cargo/config.toml"), configuration);
      if (!tracked) {
        writeFileSync(configuration, readFileSync(configuration, "utf8")
          .replace(/^ACYCLIC_BUILD_SOURCE_ROOT\s*=.*\r?\n?/gm, ""));
      }
      utimesSync(configuration, oldTime, oldTime);
      // The production configuration selects this workspace-relative linker.
      mkdirSync(join(directory, "scripts"));
      copyFileSync(join(root, "scripts/mold-linker.sh"), join(directory, "scripts/mold-linker.sh"));
      for (const [relative, contents] of Object.entries(files)) {
        const file = join(directory, relative);
        writeFileSync(file, contents);
        // Both source trees predate either build. Freshness must follow the
        // actual caller root rather than accidentally relying on newer mtimes.
        utimesSync(file, oldTime, oldTime);
      }
    }
    requireSuccess(cargo(directories[0], ["generate-lockfile", "--offline"]), `${name} lock`);
    copyFileSync(join(directories[0], "Cargo.lock"), join(directories[1], "Cargo.lock"));
    for (const [index, directory] of directories.entries()) {
      const result = cargo(directory, ["run", "--locked", "--offline", "--quiet"]);
      const expected = String(index + 11);
      if (index === 1 && !tracked) {
        assert.ok(result.status !== 0 || result.stdout.trim() !== expected,
          "removing the workspace source witness did not expose stale generation");
        console.log("PROTO_SOURCE_UNBOUND_MUTATION_REJECTED");
      } else {
        requireSuccess(result, `${name} ${index}`);
        assert.equal(result.stdout.trim(), expected, "generated schema belongs to another source root");
      }
    }
    if (tracked) console.log("PROTO_SOURCE_TRACKED_ROOTS_PASS");
  }
} finally {
  // The only deleted tree is the direct temporary child this invocation made.
  assert.equal(dirname(resolve(temporary)), parent);
  rmSync(temporary, { recursive: true, force: true });
}
