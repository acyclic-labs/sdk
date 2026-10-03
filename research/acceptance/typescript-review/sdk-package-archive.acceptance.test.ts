import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { isAbsolute, join, relative, resolve } from "node:path";

type RustReceipt = {
  status?: string;
  source_revision?: string;
  source_path?: string;
  package_artifact_path?: string;
  package_artifact_sha256?: string;
  compile_artifact_path?: string;
  compile_artifact_sha256?: string;
  package_resolution?: {
    status?: string;
    package_artifact_path?: string;
    package_artifact_sha256?: string;
    package_name?: string;
    source_revision?: string;
    source_closure_sha256?: string;
    artifact_consumed?: boolean;
    artifact_consumption_command?: string;
    command?: string;
    package_root_path?: string;
    package_manifest_path?: string;
    consumer_manifest_path?: string;
    consumer_lock_path?: string;
  };
};

type Snippet = {
  language?: string;
  source?: string;
  source_closure_sha256?: string;
  source_sha256?: string;
  code_sha256?: string;
  validation?: { receipt?: RustReceipt };
};

type ExamplesManifest = {
  snippets?: Snippet[];
  source?: { revision?: string };
};

const outputRoot = process.env.SDK_GENERATION_OUTPUT;
const expectedFailure = process.env.SDK_GENERATION_EXPECTED_FAILURE === "package-archive";

function sha256(bytes: Uint8Array): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function resolveOutputPath(root: string, portablePath: string): string {
  const candidate = isAbsolute(portablePath) ? resolve(portablePath) : resolve(root, portablePath);
  const escaped = relative(resolve(root), candidate).startsWith("..") || isAbsolute(relative(resolve(root), candidate));
  if (escaped) throw new Error(`receipt path escapes generation output: ${portablePath}`);
  return candidate;
}

function packageManifestErrors(prefix: string, packageManifest: string): string[] {
  const errors: string[] = [];
  if (/name\s*=\s*["']sdk-example-consumer["']/.test(packageManifest)) {
    errors.push(`${prefix}: package Cargo.toml names the generic test consumer`);
  }
  if (/(?:path\s*=\s*["'][A-Za-z]:[\\/]|path\s*=\s*["'](?:\\\\|\/))/.test(packageManifest)) {
    errors.push(`${prefix}: package Cargo.toml contains an absolute source dependency`);
  }
  return errors;
}

async function validateRustPackageArtifacts(root: string): Promise<string[]> {
  const manifestPath = join(root, "sdk-examples-manifest.json");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as ExamplesManifest;
  const receipts = new Map<string, { receipt: RustReceipt; snippet: Snippet }>();
  for (const snippet of manifest.snippets ?? []) {
    if (snippet.language !== "rust" || !snippet.validation?.receipt?.package_artifact_path) continue;
    receipts.set(snippet.validation.receipt.package_artifact_path, { receipt: snippet.validation.receipt, snippet });
  }

  const errors: string[] = [];
  for (const [portablePackagePath, binding] of receipts) {
    const { receipt, snippet } = binding;
    const resolution = receipt.package_resolution;
    const packagePath = resolveOutputPath(root, portablePackagePath);
    let packageBytes: Buffer;
    try {
      packageBytes = await readFile(packagePath);
    } catch (error) {
      errors.push(`${portablePackagePath}: package artifact is unreadable: ${String(error)}`);
      continue;
    }
    if (packageBytes.length < 2 || packageBytes[0] !== 0x1f || packageBytes[1] !== 0x8b) {
      const header = packageBytes.subarray(0, 16).toString("hex");
      errors.push(`${portablePackagePath}: expected gzip archive header 1f8b, got ${header}`);
    }
    const listed = spawnSync("tar", ["-tzf", packagePath], { encoding: "utf8" });
    if (listed.status !== 0) {
      errors.push(`${portablePackagePath}: tar archive listing failed: ${(listed.stderr || listed.stdout).trim()}`);
    } else if (!listed.stdout.split(/\r?\n/).some(entry => /(^|\/)Cargo\.toml$/.test(entry))) {
      errors.push(`${portablePackagePath}: archive has no Cargo.toml member`);
    } else {
      const cargoMember = listed.stdout.split(/\r?\n/).find(entry => /(^|\/)Cargo\.toml$/.test(entry));
      if (cargoMember) {
        const archivedManifest = spawnSync("tar", ["-xOf", packagePath, cargoMember], { encoding: "utf8" });
        if (archivedManifest.status !== 0) {
          errors.push(`${portablePackagePath}: archive Cargo.toml cannot be read`);
        } else {
          errors.push(...packageManifestErrors(`${portablePackagePath}: archived package`, archivedManifest.stdout));
        }
      }
    }
    const actualPackageHash = sha256(packageBytes);
    if (receipt.package_artifact_sha256 !== actualPackageHash) {
      errors.push(`${portablePackagePath}: receipt package hash ${receipt.package_artifact_sha256} does not match ${actualPackageHash}`);
    }
    if (receipt.package_resolution?.package_artifact_path !== portablePackagePath) {
      errors.push(`${portablePackagePath}: package_resolution artifact path disagrees with the snippet receipt`);
    }
    if (receipt.package_resolution?.package_artifact_sha256 !== actualPackageHash) {
      errors.push(`${portablePackagePath}: package_resolution artifact hash does not match archive bytes`);
    }
    if (receipt.compile_artifact_path && receipt.compile_artifact_sha256 === actualPackageHash) {
      errors.push(`${portablePackagePath}: package archive is byte-identical to the compile artifact`);
    }
    for (const field of ["package_root_path", "package_manifest_path", "consumer_manifest_path", "consumer_lock_path"] as const) {
      const portablePath = resolution?.[field];
      if (!portablePath) {
        errors.push(`${portablePackagePath}: package resolution omitted ${field}`);
        continue;
      }
      try {
        await stat(resolveOutputPath(root, portablePath));
      } catch {
        errors.push(`${portablePackagePath}: package resolution path is missing: ${portablePath}`);
      }
    }
    if (resolution?.package_name === "sdk-example-consumer") {
      errors.push(`${portablePackagePath}: package bytes resolve to the generic sdk-example-consumer test crate`);
    }
    if (manifest.source?.revision && receipt.source_revision !== manifest.source.revision) {
      errors.push(`${portablePackagePath}: receipt source_revision does not match manifest source revision`);
    }
    if (manifest.source?.revision && resolution?.source_revision !== manifest.source.revision) {
      errors.push(`${portablePackagePath}: package resolution source_revision is missing or mismatched`);
    }
    if (snippet.source_closure_sha256 && resolution?.source_closure_sha256 !== snippet.source_closure_sha256) {
      errors.push(`${portablePackagePath}: package resolution does not bind the snippet source closure`);
    }
    const artifactName = portablePackagePath.split(/[\\/]/).pop() ?? portablePackagePath;
    const consumptionCommand = `${resolution?.artifact_consumption_command ?? ""} ${resolution?.command ?? ""}`;
    if (resolution?.artifact_consumed !== true && !consumptionCommand.includes(artifactName)) {
      errors.push(`${portablePackagePath}: consumer receipt does not prove consumption of the declared package artifact`);
    }
    const packageManifestPath = resolution?.package_manifest_path;
    if (packageManifestPath) {
      try {
        const packageManifest = await readFile(resolveOutputPath(root, packageManifestPath), "utf8");
        errors.push(...packageManifestErrors(`${portablePackagePath}: package root`, packageManifest));
      } catch {
        // The path existence error above is the authoritative diagnostic.
      }
    }
    if (receipt.status === "qualified" && errors.some(error => error.startsWith(`${portablePackagePath}:`))) {
      errors.push(`${portablePackagePath}: receipt claims qualified despite invalid package artifact`);
    }
  }
  if (receipts.size === 0) errors.push("manifest has no Rust package artifact receipts");
  return errors;
}

test.skipIf(!outputRoot || expectedFailure)("retained Rust package artifacts are real archives with consistent receipt mappings", async () => {
  const errors = await validateRustPackageArtifacts(outputRoot!);
  expect(errors).toEqual([]);
});

test.skipIf(!outputRoot || !expectedFailure)("failed output rejects executable bytes masquerading as Rust package archives", async () => {
  const errors = await validateRustPackageArtifacts(outputRoot!);
  expect(errors.join("\n")).toContain("expected gzip archive header");
  expect(errors.join("\n")).toContain("byte-identical to the compile artifact");
  expect(errors.join("\n")).toContain("generic sdk-example-consumer test crate");
  expect(errors.join("\n")).toContain("does not prove consumption of the declared package artifact");
  expect(errors.join("\n")).toContain("does not bind the snippet source closure");
  expect(errors.join("\n")).toContain("receipt claims qualified despite invalid package artifact");
});
