import { nativeFamily } from "./native-family.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";
const assembler = createNativeAssembler(nativeFamily("stream"));
export const { assertNativeSet, sourceNativeInventory, verifyNativeAssembly, assertNativeReceipt, qualifiedCompanionManifest } = assembler;
// Preserve the existing positional Stream release entrypoint.
if (import.meta.main) {
  if (!["inventory", "release"].includes(process.argv[2]) && !process.argv[2]?.startsWith("--")) process.argv.splice(2, 0, "release");
  await assembler.main();
}
