import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { checkRustContractGenerator, runRustContractGenerator } from "./run-rust-contract-generator.mjs";

const scriptPath = fileURLToPath(import.meta.url);
const files = ["token-operations.ts"];
const relativeDirectory = "typescript/packages/stream/src";
export function generateStreamTokenOperations(root, outputDirectory) { runRustContractGenerator(root, "stream-write", outputDirectory); }
export function checkStreamTokenOperations(root) { checkRustContractGenerator(root, "stream-check", files, relativeDirectory); }
if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkStreamTokenOperations(root);
  else if (mode === "write") generateStreamTokenOperations(root);
  else throw new Error(`unknown mode: ${mode}`);
}
