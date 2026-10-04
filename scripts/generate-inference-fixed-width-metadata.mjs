import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { checkRustContractGenerator, runRustContractGenerator } from "./run-rust-contract-generator.mjs";

const scriptPath = fileURLToPath(import.meta.url);
const files = ["fixed-width-metadata.js", "fixed-width-metadata.d.ts"];
const relativeDirectory = "typescript/packages/inference/generated";
export function generateInferenceFixedWidthMetadata(root, outputDirectory) { runRustContractGenerator(root, "inference-write", outputDirectory); }
export function checkInferenceFixedWidthMetadata(root) { checkRustContractGenerator(root, "inference-check", files, relativeDirectory); }
if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkInferenceFixedWidthMetadata(root);
  else if (mode === "write") generateInferenceFixedWidthMetadata(root);
  else throw new Error(`unknown mode: ${mode}`);
}
