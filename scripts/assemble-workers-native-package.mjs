import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";
const assembler = createNativeAssembler(nativeFamily("workers"));
export const { assertNativeSet, assertCommonBindings, qualifiedCompanionManifest, qualifiedParentManifest, sourceNativeInventory, verifyNativeAssembly, assertNativeReceipt, writeCompanionManifest } = assembler;
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await assembler.main();
