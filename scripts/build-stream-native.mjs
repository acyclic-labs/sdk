import { nativeFamily } from "./native-family.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
const producer = createNativeProducer(nativeFamily("stream"));
export const { sourceSnapshot, assertSourceSnapshot, capturedCompilerIdentity, ensureCargoTargetDirectory, deterministicRustflags, darwinAppleLdPaths, configureDarwinAppleLd, withDeterministicRustflags, linkerInputs, darwinRustObjcopyIdentity, createRustcInvocationCapture, napiGeneratorIdentity, normalizeBuildInputs, buildInputsReceipt, assertBuildInputs, assertMatchingBuildInputs, rustMetadata, assertExactInventory, assertOwnedDirectory, signDarwinAddon, assertBundle, prepareBuildOutput, publishBundle } = producer;
if (import.meta.main) {
  await producer.main();
}
