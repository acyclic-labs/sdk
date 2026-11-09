import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { checkGeneratedDirectory } from "./generated-bindings.mjs";
import { generate } from "./proto-codegen.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageGenerated = join(root, "typescript/packages/actors/src/generated");

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", stdio: "inherit", windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} exited with ${result.status ?? "unknown"}`);
}

async function rustGenerate(output) {
  run("cargo", ["run", "--locked", "-p", "acyclic-actors", "--example", "actors-http-routes", "--", output]);
  await serviceGenerate("Actors", dirname(output), output);
  await writeFile(join(dirname(output), "readonly.ts"), generate(["readonly"]));
}

export async function write(check = false) {
  const output = check ? await mkdtemp(join(tmpdir(), "actors-generated-")) : packageGenerated;
  try {
    await mkdir(output, { recursive: true });
    await rustGenerate(join(output, "actors-service.ts"));
    if (check) checkGeneratedDirectory(output, packageGenerated);
  } finally {
    if (check) await rm(output, { recursive: true, force: true });
  }
}

export async function serviceGenerate(family, directory, output) {
  const lower = family.toLowerCase();
  const descriptor = join(directory, `${family}.service.bin`);
  const names = join(directory, `${family}.semantic-names.json`);
  const generated = generate(["service",
    descriptor, names, `acyclic.${lower}.v1.${family}Service`, family,
    `../../generated/proto/${lower}/v1/${lower}_pb.js`, `./semantic/${lower}/index.js`, "../client.js"],
    { stdio: ["ignore", "pipe", "inherit"] });
  await writeFile(output, generated);
  const importer = family === "Actors" ? "../client.js" : "../binding.js";
  const absence = generate(["native-absence",
    "../generated/native/binding.cjs", importer, `@acyclic-labs/${lower}-`],
    { stdio: ["ignore", "pipe", "inherit"] });
  await writeFile(join(directory, "native-absence.ts"), absence);
  await rm(descriptor);
  await rm(names);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (![undefined, "write", "rust"].includes(process.argv[2])) throw new Error("usage: generate-actors.mjs [write]");
  await write();
}
