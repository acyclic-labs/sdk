import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";

if (process.argv.length !== 8) {
  throw new Error(
    "usage: normalize-harness-evidence.mjs RUST_LOG TYPESCRIPT_LOG OUTPUT.json NPM.tgz STREAM.crate HARNESS.crate",
  );
}
const [rustPath, typescriptPath, outputPath, ...artifactPaths] = process.argv.slice(2).map(path => resolve(path));
const rust = readFileSync(rustPath, "utf8");
const typescript = readFileSync(typescriptPath, "utf8");
const collect = (source, pattern) => [...source.matchAll(pattern)].map(match => match[1]).sort();
const rustTests = collect(rust, /^test (\S+) \.\.\. ok\r?$/gm);
const typescriptTests = collect(typescript, /^\(pass\) (.+?)(?: \[[^\]]+\])?$/gm);
if (rustTests.length === 0 || typescriptTests.length === 0) {
  throw new Error("package test logs did not contain successful Rust and TypeScript cases");
}
const unique = values => [...new Set(values)];
const rustCases = unique(rustTests);
const typescriptCases = unique(typescriptTests);
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const normalizedTranscript = cases => Buffer.from(`${cases.join("\n")}\n`);
const artifacts = artifactPaths
  .map(path => ({ name: basename(path), sha256: sha256(readFileSync(path)) }))
  .sort((left, right) => left.name.localeCompare(right.name));
if (new Set(artifacts.map(artifact => artifact.name)).size !== artifacts.length) {
  throw new Error("package artifact names must be unique");
}
writeFileSync(outputPath, `${JSON.stringify({
  protocol: "acyclic.package-evidence.v1",
  artifacts,
  test_transcript_sha256: {
    rust: sha256(normalizedTranscript(rustCases)),
    typescript: sha256(normalizedTranscript(typescriptCases)),
  },
  rust_tests: rustCases,
  typescript_tests: typescriptCases,
}, null, 2)}\n`);
