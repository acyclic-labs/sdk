import { spawnSync } from "node:child_process";
import { cp, mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageGenerated = join(root, "typescript/packages/actors/src/generated");
const packageProto = join(root, "typescript/packages/actors/generated/proto/actors/v1");
const rustProto = join(root, "proto/actors/v1/actors.proto");
const buf = process.env.ACYCLIC_BUF_BIN
  ?? join(root, "node_modules/.bin", process.platform === "win32" ? "buf.exe" : "buf");

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", stdio: "inherit", windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} exited with ${result.status ?? "unknown"}`);
}

async function files(directory) {
  const result = [];
  async function visit(current) {
    for (const entry of (await readdir(current, { withFileTypes: true })).sort((left, right) => left.name.localeCompare(right.name))) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) result.push(path);
    }
  }
  await visit(directory);
  return result.sort().map(path => path.slice(directory.length + 1).split(sep).join("/"));
}

export async function assertTreeEqual(expected, actual, label) {
  const expectedFiles = await files(expected);
  const actualFiles = await files(actual);
  if (JSON.stringify(expectedFiles) !== JSON.stringify(actualFiles)) {
    throw new Error(`${label} file set drift: expected ${expectedFiles.join(", ")}; got ${actualFiles.join(", ")}`);
  }
  for (const relativePath of expectedFiles) {
    const left = await readFile(join(expected, relativePath));
    const right = await readFile(join(actual, relativePath));
    if (!left.equals(right)) throw new Error(`${label} drift: ${relativePath}`);
  }
}

async function rustGenerate(output, protoRoot) {
  await mkdir(protoRoot, { recursive: true });
  run("cargo", [
    "run", "--offline", "--locked", "-p", "acyclic-actors", "--example", "actors-http-routes", "--",
    output, "--proto-out", protoRoot,
  ]);
}

async function copyRustProto(protoRoot, destination) {
  await mkdir(dirname(destination), { recursive: true });
  await cp(join(protoRoot, "actors/v1/actors.proto"), destination);
}

async function bufActorOutput(protoFile) {
  const temporary = await mkdtemp(join(root, ".tmp-actors-buf-"));
  try {
    await mkdir(join(temporary, "proto/actors/v1"), { recursive: true });
    await cp(protoFile, join(temporary, "proto/actors/v1/actors.proto"));
    await writeFile(join(temporary, "buf.yaml"), "version: v2\nmodules:\n  - path: proto\n");
    const plugin = process.env.ACYCLIC_PROTOC_GEN_ES
      ? [process.env.ACYCLIC_PROTOC_GEN_ES.replaceAll("\\", "/")]
      : ["bun", "x", "protoc-gen-es"];
    await writeFile(join(temporary, "buf.gen.yaml"), [
      "version: v2",
      "plugins:",
      `  - local: [${plugin.map(argument => JSON.stringify(argument)).join(", ")}]`,
      "    out: generated/typescript",
      "    strategy: all",
      "    opt:",
      "      - target=js+dts",
      "      - import_extension=js",
      "",
    ].join("\n"));
    run(buf, ["generate"], temporary);
    for (const relativePath of ["generated/typescript/actors/v1/actors_pb.js", "generated/typescript/actors/v1/actors_pb.d.ts"]) {
      const path = join(temporary, relativePath);
      await writeFile(path, (await readFile(path, "utf8")).replace(/\n+$/u, "\n"));
    }
    return temporary;
  } catch (error) {
    await rm(temporary, { recursive: true, force: true });
    throw error;
  }
}

async function assertProtoOutput(actualRoot) {
  const expectedProto = await readFile(rustProto);
  const actualProto = await readFile(join(actualRoot, "actors/v1/actors.proto"));
  if (!expectedProto.equals(actualProto)) throw new Error("Actors Rust-rendered Proto drift");
}

async function check() {
  const temporary = await mkdtemp(join(root, ".tmp-actors-check-"));
  try {
    const generatedRoot = join(temporary, "generated");
    const protoRoot = join(temporary, "proto");
    await rustGenerate(join(generatedRoot, "actors-service.ts"), protoRoot);
    await assertTreeEqual(packageGenerated, generatedRoot, "Actors Rust TypeScript generation");
    await assertProtoOutput(protoRoot);
    const bufRoot = await bufActorOutput(join(protoRoot, "actors/v1/actors.proto"));
    try {
      await assertTreeEqual(packageProto, join(bufRoot, "generated/typescript/actors/v1"), "Actors Buf TypeScript generation");
    } finally {
      await rm(bufRoot, { recursive: true, force: true });
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

async function write() {
  const temporary = await mkdtemp(join(root, ".tmp-actors-write-"));
  try {
    const protoRoot = join(temporary, "proto");
    await rustGenerate(join(packageGenerated, "actors-service.ts"), protoRoot);
    await copyRustProto(protoRoot, rustProto);
    const bufRoot = await bufActorOutput(rustProto);
    try {
      await rm(packageProto, { recursive: true, force: true });
      await mkdir(packageProto, { recursive: true });
      await cp(join(bufRoot, "generated/typescript/actors/v1"), packageProto, { recursive: true });
    } finally {
      await rm(bufRoot, { recursive: true, force: true });
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

async function rustOnly() {
  const temporary = await mkdtemp(join(root, ".tmp-actors-rust-"));
  try {
    const protoRoot = join(temporary, "proto");
    await rustGenerate(join(packageGenerated, "actors-service.ts"), protoRoot);
    await copyRustProto(protoRoot, rustProto);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

export { check, rustGenerate, rustOnly, write };

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const mode = process.argv[2] ?? "check";
  if (mode === "check") await check();
  else if (mode === "write") await write();
  else if (mode === "rust") await rustOnly();
  else throw new Error(`usage: node scripts/generate-actors.mjs [check|write|rust]`);
}
