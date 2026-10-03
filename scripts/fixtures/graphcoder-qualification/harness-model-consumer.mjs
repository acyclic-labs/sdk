import { createRequire } from "node:module";
import { deepStrictEqual } from "node:assert/strict";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index < 0 ? fallback : process.argv[index + 1];
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function loadExport(consumerRoot, specifier) {
  const require = createRequire(resolve(consumerRoot, "package.json"));
  return import(pathToFileURL(require.resolve(specifier)).href);
}

function equalBytes(left, right) {
  return Array.from(left).every((byte, index) => byte === right[index]) && left.length === right.length;
}

async function rejects(label, operation) {
  try {
    await operation();
  } catch (error) {
    assert(error instanceof Error, `${label}: rejection was not an Error`);
    return error;
  }
  throw new Error(`${label}: provider admission unexpectedly succeeded`);
}

function provider(captures, dispatches = undefined) {
  return {
    async *generate(request) {
      if (dispatches !== undefined) dispatches.count += 1;
      captures.push(request);
      yield { kind: "content", delta: "admitted" };
      yield { kind: "completed", metadata: {} };
    },
    async reconcile() { return undefined; },
  };
}

function identity(revision, options) {
  return { provider: "fixture-provider", name: "canonical-model", revision, options };
}

function runtime(api, contracts, model, captures, configure = () => undefined) {
  const builder = api.Harness.builder(contracts).model(model, provider(captures));
  configure(builder);
  return builder.build();
}

const consumerRoot = option("--root");
if (consumerRoot === undefined) throw new Error("usage: node harness-model-consumer.mjs --root <installed-consumer>");

const api = await loadExport(consumerRoot, "@acyclic-labs/harness");
const contracts = await api.NativeContracts.create();
const options = {
  temperature: 0.2,
  top_p: 0.95,
  exact_integer: 9007199254740993n,
  nested: { enabled: true, labels: ["alpha", "beta"], absent: null },
};
const firstModel = identity("revision-a", options);
const firstCaptures = [];
const firstRuntime = runtime(api, contracts, firstModel, firstCaptures);
const firstOutput = await firstRuntime.run("canonical input");
assert(firstOutput.text === "admitted", "PKG-HARNESS-MODEL-01 provider output was not returned");
assert(firstCaptures.length === 1, "PKG-HARNESS-MODEL-01 expected one provider dispatch");

const firstRequest = firstCaptures[0];
const evidence = firstRequest.canonical;
assert(evidence !== undefined, "PKG-HARNESS-MODEL-01 provider request omitted canonical evidence");
assert(Object.isFrozen(evidence) && Object.isFrozen(evidence.requestDigest), "PKG-HARNESS-MODEL-01 canonical evidence is mutable");
assert(typeof evidence.requestJson === "string" && evidence.requestJson.length > 0, "PKG-HARNESS-MODEL-01 request JSON is empty");
assert(typeof evidence.manifestJson === "string" && evidence.manifestJson.length > 0, "PKG-HARNESS-MODEL-01 manifest JSON is empty");
assert(Array.isArray(evidence.requestDigest) && evidence.requestDigest.length === 32, "PKG-HARNESS-MODEL-01 digest length is invalid");

const requestDocument = contracts.decodeCanonicalJson(new TextEncoder().encode(evidence.requestJson));
assert(!Object.hasOwn(requestDocument, "canonical"), "PKG-HARNESS-MODEL-01 transport evidence leaked into model request JSON");
assert(requestDocument.model.provider === firstModel.provider, "model provider identity was changed");
assert(requestDocument.model.name === firstModel.name, "model name identity was changed");
assert(requestDocument.model.revision === firstModel.revision, "model revision identity was changed");
deepStrictEqual(requestDocument.model.options, firstModel.options);
assert(requestDocument.messages.length === 1 && requestDocument.messages[0].role === "user", "canonical message role was changed");
assert(requestDocument.messages[0].content === "canonical input", "canonical message content was changed");
assert(Array.isArray(requestDocument.tools) && requestDocument.tools.length === 0, "unexpected model tools entered the request");
assert(equalBytes(contracts.canonicalJsonDigest(requestDocument), evidence.requestDigest), "request digest does not cover the exact canonical request");

const manifest = contracts.decodeCanonicalJson(new TextEncoder().encode(evidence.manifestJson));
assert(equalBytes(manifest.request_digest, evidence.requestDigest), "manifest request identity differs from provider request identity");
assert(Array.isArray(manifest.messages) && manifest.messages.length === 1 && manifest.messages[0].position === 0, "manifest message identity is not ordered");
assert(!evidence.manifestJson.includes("canonical input"), "manifest duplicated model-visible content instead of retaining identity");

const secondCaptures = [];
const secondModel = identity("revision-b", { ...options, top_p: 0.94 });
await runtime(api, contracts, secondModel, secondCaptures).run("canonical input");
const secondEvidence = secondCaptures[0].canonical;
const secondDocument = contracts.decodeCanonicalJson(new TextEncoder().encode(secondEvidence.requestJson));
const secondManifest = contracts.decodeCanonicalJson(new TextEncoder().encode(secondEvidence.manifestJson));
assert(secondDocument.model.revision === "revision-b" && secondDocument.model.options.top_p === 0.94, "model identity/options were not lossless on the second request");
assert(evidence.requestJson !== secondEvidence.requestJson, "canonical request JSON did not bind model identity");
assert(!equalBytes(evidence.requestDigest, secondEvidence.requestDigest), "request digest did not change with model identity");
assert(!equalBytes(manifest.binding_digest, secondManifest.binding_digest), "binding digest did not change with model identity");

let orphanDispatches = { count: 0 };
const orphanCaptures = [];
const orphanBuilder = api.Harness.builder(contracts).model(identity("orphan-negative", options), provider(orphanCaptures, orphanDispatches)).context({
  async build() {
    return [
      { role: "assistant", content: { kind: "tool_call", callId: "orphan", name: "unregistered", arguments: { value: "x" } } },
      { role: "user", content: "safe" },
    ];
  },
});
await rejects("NEG-MODEL-PAIR-01", () => orphanBuilder.build().run("safe"));
assert(orphanDispatches.count === 0, "NEG-MODEL-PAIR-01 dispatched an unpaired context before admission");

let boundedDispatches = { count: 0 };
const boundedBuilder = api.Harness.builder(contracts).limits({ file_bytes: 16, render_bytes: 16 }).model(identity("bounds-negative", options), {
  async *generate() {
    boundedDispatches.count += 1;
    yield { kind: "completed", metadata: {} };
  },
  async reconcile() { return undefined; },
}).context({
  async build() {
    return [
      { role: "user", content: "12345678" },
      { role: "assistant", content: "87654321" },
    ];
  },
});
await rejects("NEG-MODEL-BOUNDS-01", () => boundedBuilder.build().run("safe"));
assert(boundedDispatches.count === 0, "NEG-MODEL-BOUNDS-01 dispatched an oversized aggregate context");

process.stdout.write(JSON.stringify({
  ok: true,
  scenarios: ["PKG-HARNESS-MODEL-01", "NEG-MODEL-PAIR-01", "NEG-MODEL-BOUNDS-01"],
}) + "\n");
