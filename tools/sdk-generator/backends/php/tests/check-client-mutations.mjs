import { cpSync, existsSync, lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";
import { qualify } from "../src/qualify.mjs";
import { sha256 } from "../../../shared/authority.mjs";

// Optional installed-runtime controls; routine CI needs no downloaded PHP tools.
const options = Object.fromEntries(["package", "authority", "php-home", "composer", "grpc-extension", "archiver", "cache", "output"].map(name => [name, { type: "string" }]));
const { values } = parseArgs({ options });
if (Object.keys(options).some(name => !values[name])) throw new Error("all qualification paths are required");
const source = realpathSync(values.package);
const output = join(realpathSync(dirname(resolve(values.output))), basename(resolve(values.output)));
try { lstatSync(output); throw new Error("mutation output must be absent"); }
catch (error) { if (error.code !== "ENOENT") throw error; }
const within = (parent, child) => {
  const path = relative(parent.toLowerCase(), child.toLowerCase());
  return path === "" || (path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path));
};
for (const input of [source, ...Object.keys(options).filter(name => !["package", "output"].includes(name)).map(name => realpathSync(values[name])), resolve(dirname(fileURLToPath(import.meta.url)), "..")]) {
  if (within(input, output) || within(output, input)) throw new Error("mutation output overlaps an input");
}
mkdirSync(output);
const baseline = qualify({ ...values, output: join(output, "baseline") });
const file = "src/Acyclic/Actors/V1/ActorsServiceClient.php";
const mutations = [
  ["path", "'/acyclic.actors.v1.ActorsService/CreateActor'", "'/acyclic.actors.v1.ActorsService/WrongActor'", "client RPC path differs"],
  ["request", "        $argument,", "        new \\Acyclic\\Actors\\V1\\CreateActorRequest(),", "client request content differs"],
  ["in-place-request", "        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/CreateActor',", "        $argument->setHomeRegion('mutated-by-generated-client');\n        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/CreateActor',", "client request content differs"],
  ["response", "['\\Acyclic\\Actors\\V1\\CreateActorResponse', 'decode']", "['\\Acyclic\\Actors\\V1\\InspectActorResponse', 'decode']", "client response type differs"],
  ["streaming", "return $this->_simpleRequest(", "return $this->_serverStreamRequest(", "client RPC streaming shape differs"],
];
const results = [];
for (const [name, before, after, diagnostic] of mutations) {
  const packageRoot = join(output, `${name}-package`), qualification = join(output, `${name}-qualification`);
  cpSync(source, packageRoot, { recursive: true });
  const original = readFileSync(join(packageRoot, file), "utf8");
  if (!original.includes(before)) throw new Error(`mutation target missing: ${name}`);
  writeFileSync(join(packageRoot, file), original.replace(before, after));
  const receiptPath = join(packageRoot, "generation-receipt.json"), receipt = JSON.parse(readFileSync(receiptPath));
  receipt.output_sha256[file] = sha256(readFileSync(join(packageRoot, file)));
  writeFileSync(receiptPath, JSON.stringify(receipt));
  let failure;
  try { qualify({ ...values, package: packageRoot, output: qualification }); }
  catch (error) { failure = error.message; }
  const log = join(qualification, "positive.log");
  if (!failure || existsSync(join(qualification, "qualification.json")) || !existsSync(log) || !readFileSync(log, "utf8").includes(diagnostic)) throw new Error(`mutation did not fail as intended: ${name}: ${failure}`);
  results.push({ name, diagnostic, failure, client_sha256: receipt.output_sha256[file], positive_log_sha256: sha256(readFileSync(log)) });
  console.log(`PASS rejected ${name}: ${diagnostic}`);
}
writeFileSync(join(output, "mutation-results.json"), JSON.stringify({
  source_revision: baseline.source_revision, archive_sha256: baseline.archive_sha256,
  qualifier_sha256: baseline.qualifier_sha256, runner_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), results,
}, null, 2) + "\n");
