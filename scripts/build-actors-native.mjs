import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
const producer = createNativeProducer(nativeFamily("actors"));
export const { assertExactInventory, assertSelectedArtifact, normalizeBuildInputs, buildInputsReceipt } = producer;
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === "build") execFileSync(process.execPath, [fileURLToPath(new URL("./generate.mjs", import.meta.url)), "actors"], { stdio: "inherit" });
  await producer.main();
}
