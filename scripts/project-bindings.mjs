// The same source projections run from a CLI's Bazel tools or an action's
// declared executable inputs. No compiler or Git lookup belongs in projection.
import {execFileSync} from "node:child_process";
import {dirname, join, resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {contractModules, keepsGenerator, loadContractModules, generateContracts} from "./generate-contracts.mjs";
import {PROTO_CODEGEN} from "./proto-codegen.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const AUTH = "//rust/crates/auth:generate_example";
const ACTORS = "//rust/crates/actors:actors-http-routes_example";
const MANIFEST = "//rust/crates/manifest:deployment-manifest_example";

export async function bindingTools(packages, {families = [...packages], native = false} = {}) {
  const generators = (await loadContractModules(contractModules(families)))
    .filter(generator => keepsGenerator(generator, families, native));
  return [...new Set([
    ...(packages.has("auth") ? [AUTH] : []),
    ...(packages.has("actors") ? [ACTORS, PROTO_CODEGEN] : []),
    ...(packages.has("workers") ? [MANIFEST] : []),
    ...generators.flatMap(generator => generator.rust ?? []),
  ])].sort();
}

export async function projectBindings(packages, {tools, check = false, families = [...packages], native = false, sourceCommit = undefined, protobuf = true, runtimeOnly = false}) {
  const [{generateFiles}, {generateProto}, {write: generateActors}] = await Promise.all([
    import("./generated-bindings.mjs"), import("./generate-proto.mjs"), import("./generate-actors.mjs"),
  ]);
  const run = (label, args, options = {}) => {
    const executable = tools.get(label);
    if (!executable) throw Error(`BindingToolMissing: ${label}`);
    return execFileSync(executable, args, {cwd:root, encoding:"utf8", maxBuffer:Infinity,
      stdio:["ignore", "pipe", "inherit"], ...options});
  };
  if (packages.has("auth")) await generateFiles(root, output => run(AUTH, [output]), [], check);
  if (protobuf) generateProto(packages, check);
  if (packages.has("actors")) await generateActors(check, {routes:() => tools.get(ACTORS), codegen:(args, options) => run(PROTO_CODEGEN, args, options)});
  if (packages.has("workers")) {
    await generateFiles(join(root, "typescript/packages/workers/src/generated"), destination => run(MANIFEST, [destination]), [], check);
    await generateFiles(join(root, "typescript/packages/workers/test/generated"), destination => run(MANIFEST, [
      destination, join(root, "rust/crates/manifest/fuzz/corpus/manifest_json/valid.json"), "production", "agent"]), [], check);
  }
  await generateContracts(check ? "check" : "write", families, {native, tools, sourceCommit, runtimeOnly});
}
