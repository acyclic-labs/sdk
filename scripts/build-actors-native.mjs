import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
const producer = createNativeProducer(nativeFamily("actors"));
export const { sourceSnapshot, assertSourceSnapshot, capturedCompilerIdentity, ensureCargoTargetDirectory, deterministicRustflags, darwinAppleLdPaths, configureDarwinAppleLd, withDeterministicRustflags, linkerInputs, darwinRustObjcopyIdentity, createRustcInvocationCapture, napiGeneratorIdentity, normalizeBuildInputs, buildInputsReceipt, buildInputs, assertBuildInputs, assertMatchingBuildInputs, rustMetadata, assertCurrentNativeFamily, assertExactInventory, assertOwnedDirectory, signDarwinAddon, assertBundle, prepareBuildOutput, publishBundle, assertSelectedArtifact } = producer;
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await producer.main();
