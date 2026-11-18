import { nativeFamily } from "./native-family-metadata.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";
const assembler = createNativeAssembler(nativeFamily("actors"));
export const { assertNativeSet, assertCommonBindings, sourceNativeInventory, verifyNativeAssembly } = assembler;
if (import.meta.main) await assembler.main();
