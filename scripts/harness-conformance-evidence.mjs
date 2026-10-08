import { isDeepStrictEqual } from "node:util";

// Packaging owns these bytes. This checks their binding, not their authenticity.
export function verifyPackageProvenance(sourceCommit, checksums, sourceRevision, files) {
  if (sourceCommit.trim() !== sourceRevision) throw new Error("package source does not match the conformance checkout");
  const inventory = new Map();
  for (const line of checksums.trimEnd().split(/\r?\n/)) {
    const match = line.match(/^([0-9a-f]{64}) [ *]([^\\/]+)$/);
    if (!match || inventory.has(match[2])) throw new Error("invalid or duplicate package checksum entry");
    inventory.set(match[2], match[1]);
  }
  if (inventory.size !== files.size) throw new Error("package checksum inventory does not exactly cover consumed files");
  for (const [name, actual] of files) {
    if (inventory.get(name) !== actual) throw new Error(`package checksum does not match ${name}`);
  }
}

// A successful validator exit and aggregate pass count cannot stand in for the
// receipt of this exact report. Bind fields and the digest of its actual bytes.
export function verifyQualificationReceipt(report, receipt, reportDigest) {
  if (receipt.protocol !== "acyclic.conformance.receipt.v1" || receipt.qualified !== true ||
      receipt.total !== report.cases.length || receipt.passed !== receipt.total ||
      receipt.report_digest !== reportDigest || report.cases.some(item => item.status !== "passed")) {
    throw new Error("executed report did not qualify");
  }
  for (const key of ["family", "suite_version", "suite_digest", "subject", "runner", "protocol_identity", "capability_profile", "cases"]) {
    if (!isDeepStrictEqual(receipt[key], report[key])) {
      throw new Error(`qualification receipt does not bind report ${key}`);
    }
  }
}
