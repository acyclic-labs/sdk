import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const protoPath = resolve(root, "typescript/packages/actors/generated/proto/actors/v1/actors_pb.js");
const outputPath = resolve(root, "typescript/packages/actors/src/generated/actors-service.ts");

const { ActorsService } = await import(pathToFileURL(protoPath));
const methods = ActorsService.methods.map(method => ({
  operation: method.localName,
  input: method.input.typeName.split(".").at(-1),
  output: method.output.typeName.split(".").at(-1),
}));

if (methods.length === 0 || methods.some(method => !method.operation || !method.input || !method.output)) {
  throw new Error("ActorsService descriptor did not expose complete method metadata");
}

const operations = methods.map(method => JSON.stringify(method.operation)).join(" | ");
const typeMethods = methods.map(method =>
  `  readonly ${method.operation}: (request: ReadonlySemantic<Semantic.${method.input}>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.${method.output}>>;`,
).join("\n");

const source = `// Generated from the canonical ActorsService descriptor. Do not edit.\nimport { ActorsService } from "../../generated/proto/actors/v1/actors_pb.js";\nimport type { ActorsCallOptions, ReadonlySemantic } from "../client.js";\nimport type * as Semantic from "./semantic/actors/index.js";\n\nexport type ActorsMethod = (typeof ActorsService.methods)[number];\nexport type ActorsOperation = ${operations};\nexport const ACTORS_OPERATION_NAMES = Object.freeze(ActorsService.methods.map(method => method.localName)) as readonly ActorsOperation[];\n\nexport interface ActorsClientMethods {\n${typeMethods}\n}\n\nexport type ActorsMethodCall = (method: ActorsMethod, request: unknown, options?: ActorsCallOptions) => Promise<unknown>;\n\n/** Installs the typed public methods from the maintained service descriptor. */\nexport function installActorsMethods(target: object, call: ActorsMethodCall): void {\n  for (const method of ActorsService.methods) {\n    Object.defineProperty(target, method.localName, {\n      configurable: true,\n      enumerable: true,\n      value: (request: unknown, options?: ActorsCallOptions) => call(method, request, options),\n    });\n  }\n}\n`;

await mkdir(dirname(outputPath), { recursive: true });
const existing = await readFile(outputPath, "utf8").catch(() => undefined);
if (existing !== source) await writeFile(outputPath, source);
