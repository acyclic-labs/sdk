import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

export function validatePlatformQualification(receipt, expectedPackage, expectedRevision) {
  if (receipt?.schema !== "acyclic.sdk.platform-qualification.v1") throw new Error("Invalid platform qualification schema");
  if (!["cpp", "swift"].includes(expectedPackage) || receipt.package !== expectedPackage) throw new Error("Platform package mismatch");
  if (!/^[a-f0-9]{40}$/i.test(expectedRevision ?? "") || receipt.source_revision !== expectedRevision) throw new Error("Platform source revision mismatch");
  if (receipt.status !== "qualified") throw new Error(`${expectedPackage} qualification is ${receipt.status ?? "missing"}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const [receiptPath, expectedPackage, expectedRevision] = process.argv.slice(2);
    validatePlatformQualification(JSON.parse(readFileSync(receiptPath, "utf8")), expectedPackage, expectedRevision);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
