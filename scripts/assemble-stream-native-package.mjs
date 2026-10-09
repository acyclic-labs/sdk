import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";
const assembler = createNativeAssembler(nativeFamily("stream"));
export const { assertNativeSet, assertCommonBindings, qualifiedCompanionManifest, qualifiedParentManifest, sourceNativeInventory, verifyNativeAssembly, assertNativeReceipt, writeCompanionManifest } = assembler;
// Preserve the existing positional Stream release entrypoint.
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (!["inventory", "release"].includes(process.argv[2]) && !process.argv[2]?.startsWith("--")) process.argv.splice(2, 0, "release");
  await assembler.main();
}
