import { spawnSync } from "node:child_process";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

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
  const readonly = spawnSync("cargo", ["run", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "readonly"], { cwd: root, encoding: "utf8" });
  if (readonly.error) throw readonly.error;
  if (readonly.status !== 0) throw new Error(`Readonly generation failed: ${readonly.stderr}`);
  await writeFile(join(dirname(output), "readonly.ts"), readonly.stdout);
}

export async function write() {
  await mkdir(packageGenerated, { recursive: true });
  await rustGenerate(join(packageGenerated, "actors-service.ts"));
}

export async function serviceGenerate(family, directory, output) {
  const lower = family.toLowerCase();
  const descriptor = join(directory, `${family}.service.bin`);
  const names = join(directory, `${family}.semantic-names.json`);
  const generated = spawnSync("cargo", ["run", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "service",
    descriptor, names, `acyclic.${lower}.v1.${family}Service`, family,
    `../../generated/proto/${lower}/v1/${lower}_pb.js`, `./semantic/${lower}/index.js`, "../client.js"],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], windowsHide: true });
  if (generated.error) throw generated.error;
  if (generated.status !== 0) throw new Error(`${family} semantic service projection failed`);
  await writeFile(output, generated.stdout);
  const importer = family === "Actors" ? "../client.js" : "../binding.js";
  const absence = spawnSync("cargo", ["run", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "native-absence",
    "../generated/native/binding.cjs", importer, `@acyclic-labs/${lower}-`],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], windowsHide: true });
  if (absence.error) throw absence.error;
  if (absence.status !== 0) throw new Error(`${family} native absence projection failed`);
  await writeFile(join(directory, "native-absence.ts"), absence.stdout);
  await rm(descriptor);
  await rm(names);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (![undefined, "write", "rust"].includes(process.argv[2])) throw new Error("usage: generate-actors.mjs [write]");
  await write();
}
