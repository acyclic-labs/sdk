import { createHash, randomUUID } from "node:crypto";
import { mkdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// Registry visibility has no subscription in these protocols. One observation
// is a result, not a reason to start a readiness timer. Receipts are evidence;
// reconciliation always authenticates the source/archive and queries again.
export function publicationReceipt(identity, observation, matches = (expected, observed) => expected === observed) {
  if (observation.digest !== null && !matches(identity.expectedDigest, observation.digest)) {
    throw new Error(`${identity.name}@${identity.version} exists with different bytes`);
  }
  const pending = observation.digest === null
    ? "archive not visible"
    : observation.tag && observation.tag.observed !== observation.tag.expected
      ? "tag not visible at the expected version"
      : null;
  return { schema: 1, ...identity, state: pending === null ? "Complete" : "Pending", pending, observation };
}

export class PublicationPending extends Error {
  constructor(receipt, command) {
    super(`${receipt.name}@${receipt.version}: ${receipt.pending}; reconcile once with ${command}`);
    this.receipt = receipt;
  }
}

export function retainPublication(receipt, directory) {
  mkdirSync(directory, { recursive: true });
  const key = createHash("sha256").update(JSON.stringify([
    receipt.registry, receipt.sourceSha, receipt.name, receipt.version,
  ])).digest("hex");
  const temporary = join(directory, `.${key}-${randomUUID()}.tmp`);
  try {
    writeFileSync(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
    renameSync(temporary, join(directory, `${key}.json`));
  } finally {
    rmSync(temporary, { force: true });
  }
}

export function requirePublication(receipt, directory, command) {
  retainPublication(receipt, directory);
  if (receipt.state === "Pending") throw new PublicationPending(receipt, command);
}

export async function publicationMain(action) {
  try {
    await action();
  } catch (error) {
    if (!(error instanceof PublicationPending)) throw error;
    console.error(error.message);
    console.error(JSON.stringify(error.receipt));
    process.exitCode = 75;
  }
}
