import { createHash, randomUUID } from "node:crypto";
import { mkdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// Registry indexing has no push subscription in the publication commands.
// Keep the caller's exact resume identity instead of waiting for propagation.
export class RegistryPublicationUnobservable extends Error {
  constructor(registry, name, version, observed) {
    const recovery = publicationCommand();
    super(`step=${registry}-publication-visibility ${name}@${version} is unobservable: ${JSON.stringify(observed)}; recovery command: ${recovery}`);
    this.name = "RegistryPublicationUnobservable";
    this.code = "REGISTRY_PUBLICATION_UNOBSERVABLE";
    this.registry = registry;
    this.package = name;
    this.version = version;
    this.observed = observed;
    this.recovery = recovery;
  }
}

// These commands are printed for the caller, never executed by a shell here.
export function publicationCommand(command = process.execPath, args = [...process.execArgv, ...process.argv.slice(1)]) {
  const quote = process.platform === "win32"
    ? value => `'${String(value).replaceAll("'", "''")}'`
    : value => `'${String(value).replaceAll("'", "'\"'\"'")}'`;
  return (process.platform === "win32" ? "& " : "") + [command, ...args].map(quote).join(" ");
}

// This records evidence only. Publication always authenticates the original
// qualified archive and observes the registry again; these files are never read.
function recordPublicationObservation(identity, observation, directory = "target/registry-publication") {
  const { registry, sourceSha, name, version, expectedDigest } = identity;
  const record = { $version: 1, registry, sourceSha, name, version, expectedDigest, observation };
  const key = createHash("sha256").update(JSON.stringify([registry, sourceSha, name, version, expectedDigest])).digest("hex");
  mkdirSync(directory, { recursive: true });
  const path = join(directory, key + ".json");
  const temporary = join(directory, "." + key + "-" + randomUUID() + ".tmp");
  try {
    writeFileSync(temporary, JSON.stringify(record, null, 2) + "\n", { flag: "wx", mode: 0o600 });
    renameSync(temporary, path);
  } finally {
    rmSync(temporary, { force: true });
  }
  return path;
}

// Retain the available observation even when a read or identity check fails.
// Transport messages remain with the thrown error; records contain no raw
// command diagnostics, which can include authentication configuration.
export async function observePublication(identity, action, directory) {
  const sourceSha = identity.sourceSha;
  if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(sourceSha ?? "")) {
    throw new Error("publication observation requires its qualified source identity");
  }
  const observation = {};
  let failure;
  let failed = false;
  try {
    await action(observation);
  } catch (error) {
    failed = true;
    failure = error;
    observation.error = { name: error?.name ?? "Error", code: typeof error?.code === "string" ? error.code : null };
  }
  try {
    recordPublicationObservation(identity, observation, directory);
  } catch (error) {
    if (failed) throw new AggregateError([failure, error], "Publication and observation retention failed");
    throw error;
  }
  if (failed) throw failure;
}
