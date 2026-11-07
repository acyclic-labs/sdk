import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, packagedSourceCopies, writeChanged } from "./generated-bindings.mjs";
import { generate } from "./proto-codegen.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const selected = process.argv.slice(2);
const families = selected.length ? selected : Object.keys(compatibilityArtifacts).filter(name => compatibilityArtifacts[name].descriptorDigest);
const temporary = mkdtempSync(join(root, "target-descriptors-"));
let step = "select descriptor families";
try {
  for (const family of families) {
    const artifacts = compatibilityArtifacts[family];
    if (!artifacts?.descriptorDigest) throw new Error(`unknown descriptor family: ${family}`);
    step = `compile ${family} Rust descriptor`;
    generate(["descriptors", root, temporary, family === "filesystem" ? "filesystem-wire" : family], { stdio: "inherit" });
    const bytes = readFileSync(join(temporary, artifacts.descriptorDigest.split("/").at(-1)));
    if (family === "actors") {
      step = "compile independent Actors Buf descriptor";
      const buf = join(root, "node_modules/.bin", process.platform === "win32" ? "buf.exe" : "buf");
      const image = join(temporary, "actors-buf.bin");
      execFileSync(buf, ["build", "--path", "rust/crates/actors/proto/actors", "--exclude-source-info", "-o", image],
        { cwd: root, stdio: "inherit", windowsHide: true });
      generate(["canonicalize", "actors", image, image], { stdio: "inherit" });
      writeChanged(join(root, "rust/crates/actors/tests/fixtures/actors/v1/actors-buf-v1.bin"), readFileSync(image));
    }
    step = `publish ${family} descriptor and packaged copies`;
    writeChanged(join(root, artifacts.descriptorDigest), bytes);
    for (const [source, packaged] of packagedSourceCopies) {
      if (source === artifacts.descriptorDigest) writeChanged(join(root, packaged), bytes);
    }
  }
  step = "record compatibility digests";
  execFileSync(process.execPath, [join(root, "scripts/check-compatibility-digests.mjs"), "write"],
    { cwd: root, stdio: "inherit", windowsHide: true });
} catch (error) {
  throw new Error(`descriptor capture step ${step} failed; recovery: node scripts/capture-descriptors.mjs ${families.join(" ")}`, { cause: error });
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
