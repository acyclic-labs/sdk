import { rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { buildContractTargets, captureContractCommand } from "./contract-targets.mjs";
import { generateFiles, writeChanged } from "./generated-bindings.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
if (![undefined, "write", "rust"].includes(process.argv[2])) throw new Error(
  "STEP_FAILED actors-generation-mode; recovery: node scripts/generate-actors.mjs write");
const [routes, codegen] = buildContractTargets(root, [
  ["acyclic-actors", "example", "actors-http-routes"], ["sdk-proto-codegen", "bin", "sdk-proto-codegen"],
]);
await generateFiles(join(root, "typescript/packages/actors/src/generated"), temporary => {
  const generate = args => captureContractCommand(root, codegen, args, "actors-projection");
  captureContractCommand(root, routes, [join(temporary, "actors-service.ts")], "actors-metadata");
  const descriptor = join(temporary, "Actors.service.bin");
  const names = join(temporary, "Actors.semantic-names.json");
  writeChanged(join(temporary, "actors-service.ts"), generate(["service", descriptor, names,
    "acyclic.actors.v1.ActorsService", "Actors", "../../generated/proto/actors/v1/actors_pb.js",
    "./semantic/actors/index.js", "../client.js"]));
  writeChanged(join(temporary, "native-absence.ts"), generate([
    "native-absence", "../generated/native/binding.cjs", "../client.js", "@acyclic-labs/actors-",
  ]));
  writeChanged(join(temporary, "readonly.ts"), generate(["readonly"]));
  rmSync(descriptor);
  rmSync(names);
}, ["semantic"]);
