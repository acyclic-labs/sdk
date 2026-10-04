import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { checkRustContractGenerator, runRustContractGenerator } from "./run-rust-contract-generator.mjs";

const scriptPath = fileURLToPath(import.meta.url);
const files = ["http-contract.ts"];
const relativeDirectory = "typescript/packages/stream/src";
export function generateStreamHttpContract(root, outputDirectory) { runRustContractGenerator(root, "stream-write", outputDirectory); }
export function checkStreamHttpContract(root) { checkRustContractGenerator(root, "stream-check", files, relativeDirectory); }
if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkStreamHttpContract(root);
  else if (mode === "write") generateStreamHttpContract(root);
  else throw new Error(`unknown mode: ${mode}`);
}
