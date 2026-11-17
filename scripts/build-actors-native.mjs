import { nativeFamily } from "./native-family-metadata.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
const producer = createNativeProducer(nativeFamily("actors"));
export const { assertExactInventory, assertSelectedArtifact, normalizeBuildInputs, buildInputsReceipt } = producer;
if (import.meta.main) {
  await producer.main();
}
