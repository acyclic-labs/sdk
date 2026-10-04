import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const packageRoot = join(root, "typescript", "packages", "graphcoder");
const sourceRoot = join(packageRoot, "src");
const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));

function sourceInventory(directory = sourceRoot) {
  const files = new Map();
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      for (const [nested, content] of sourceInventory(path)) files.set(nested, content);
    } else if (entry.isFile() && extname(entry.name) === ".ts") {
      files.set(resolve(path), readFileSync(path, "utf8"));
    }
  }
  return files;
}

const sources = sourceInventory();

function modulePath(path) {
  return relative(sourceRoot, path).replaceAll("\\", "/");
}

function importedSpecifiers(source) {
  const values = new Set();
  const patterns = [
    /\bimport\s+(?:type\s+)?[\s\S]*?\sfrom\s+["']([^"']+)["']/g,
    /\bexport\s+(?:type\s+)?[\s\S]*?\sfrom\s+["']([^"']+)["']/g,
    /\bimport\s*["']([^"']+)["']/g,
    /\bimport\s*\(\s*["']([^"']+)["']\s*\)/g,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) values.add(match[1]);
  }
  return values;
}

function resolveSource(from, specifier) {
  let path = resolve(dirname(from), specifier);
  if (path.endsWith(".js")) path = path.slice(0, -3) + ".ts";
  else if (extname(path) === "") path += ".ts";
  return path;
}

function dependencyGraph(entry) {
  const files = new Set();
  const nodeImports = [];
  const externalImports = [];
  const pending = [resolve(sourceRoot, entry)];
  while (pending.length > 0) {
    const current = pending.pop();
    if (files.has(current)) continue;
    const source = sources.get(current);
    assert.ok(source !== undefined, "missing GraphCoder source for " + entry + ": " + modulePath(current));
    files.add(current);
    for (const specifier of importedSpecifiers(source)) {
      if (specifier.startsWith(".")) {
        const imported = resolveSource(current, specifier);
        assert.ok(sources.has(imported), modulePath(current) + " imports missing source " + specifier);
        pending.push(imported);
      } else {
        externalImports.push({ file: modulePath(current), specifier });
        if (specifier.startsWith("node:")) nodeImports.push({ file: modulePath(current), specifier });
      }
    }
  }
  return { files, nodeImports, externalImports };
}

function sourceEntry(exportTarget) {
  assert.match(exportTarget, /^\.\/dist\/[^/]+\.js$/u);
  return exportTarget.replace(/^\.\/dist\//u, "").replace(/\.js$/u, ".ts");
}

const expectedExports = {
  ".": "./dist/index.js",
  "./terminal": "./dist/terminal.js",
  "./mock": "./dist/mock.js",
  "./bridge": "./dist/bridge.js",
  "./node": "./dist/node.js",
  "./node-dispatcher": "./dist/node-dispatcher.js",
  "./native-cli": "./dist/native-cli.js",
};

test("GraphCoder package exposes the contract and explicit host entry points", () => {
  assert.deepEqual(Object.keys(manifest.exports).sort(), Object.keys(expectedExports).sort());
  for (const [name, target] of Object.entries(expectedExports)) {
    assert.deepEqual(manifest.exports[name], {
      types: target.replace(/\.js$/u, ".d.ts"),
      default: target,
    }, "export map drift: " + name);
    assert.ok(sources.has(resolve(sourceRoot, sourceEntry(target))), "export target has no TypeScript source: " + name);
  }
  assert.deepEqual(manifest.bin, {
    graphcoder: "dist/cli.js",
    "graphcoder-native": "dist/native-cli.js",
  });
  assert.ok(sources.has(join(sourceRoot, "cli.ts")), "GraphCoder CLI bin has no source");
  assert.ok(sources.has(join(sourceRoot, "native-cli.ts")), "native CLI bin has no source");
  assert.deepEqual(Object.keys(manifest.dependencies ?? {}), [], "GraphCoder must not eagerly pull a runtime package");
  assert.deepEqual(Object.keys(manifest.optionalDependencies ?? {}), [], "optional runtime dependencies must stay host supplied");
  assert.deepEqual(Object.keys(manifest.peerDependencies ?? {}), [], "host integrations must remain injected adapters");
});

test("the root GraphCoder API is Node free and transport injected", () => {
  const graph = dependencyGraph("index.ts");
  assert.deepEqual(graph.nodeImports, [], "root export transitively imports a Node host API");
  assert.deepEqual(graph.externalImports, [], "root export must not import a web, cloud, model, storage, or Harness package");
  for (const forbidden of ["terminal.ts", "process.ts", "node.ts", "node-dispatcher.ts", "native-cli.ts", "cli.ts"]) {
    assert.equal(graph.files.has(join(sourceRoot, forbidden)), false, "root API eagerly reaches host adapter " + forbidden);
  }
  const api = sources.get(join(sourceRoot, "api.ts"));
  assert.match(api, /export\s+interface\s+GraphCoderTransport\b/u);
  assert.match(api, /constructor\(readonly transport: GraphCoderTransport\)/u);
});

test("host subpaths are lazy and own the only process integration", () => {
  const rootGraph = dependencyGraph("index.ts");
  const hostEntries = ["terminal.ts", "node.ts", "node-dispatcher.ts", "native-cli.ts", "cli.ts"];
  const allowedNodeImports = new Set([
    "node:child_process",
    "node:crypto",
    "node:process",
    "node:path",
    "node:readline/promises",
    "node:stream",
  ]);
  for (const entry of hostEntries) {
    const graph = dependencyGraph(entry);
    assert.equal(rootGraph.files.has(resolve(sourceRoot, entry)), false, entry + " is eagerly reachable from the root API");
    for (const { file, specifier } of graph.nodeImports) {
      assert.ok(allowedNodeImports.has(specifier), file + " imports forbidden host capability " + specifier);
      assert.ok(/(?:^|\/)(?:terminal|process|node|node-dispatcher|native-cli|cli|owned-process)\.ts$/u.test(file), file + " owns a Node import outside the host adapter");
    }
    assert.deepEqual(
      graph.externalImports.filter(({ specifier }) => !specifier.startsWith("node:")),
      [],
      entry + " imports an undeclared package instead of an injected host",
    );
  }
});

test("public wire contracts have one canonical source and one domain reducer per role", () => {
  const contract = sources.get(join(sourceRoot, "bridge.ts"));
  const api = sources.get(join(sourceRoot, "api.ts"));
  const dispatcher = sources.get(join(sourceRoot, "dispatcher.ts"));
  assert.match(contract, /export\s+type\s+GraphCoderWireMethod\b/u);
  assert.match(contract, /export\s+interface\s+GraphCoderBridge\b/u);
  assert.match(contract, /export\s+interface\s+GraphCoderWireParamsByMethod\b/u);
  assert.match(contract, /export\s+interface\s+GraphCoderWireResultByMethod\b/u);
  assert.match(dispatcher, /GraphCoderWireMethod/u);
  assert.match(dispatcher, /from\s+["']\.\/bridge\.js["']/u);
  assert.match(api, /export\s+class\s+GraphCoderUi\b/u);
  assert.match(contract, /export\s+class\s+BridgeGraphCoderTransport\b/u);
  assert.match(dispatcher, /export\s+class\s+GraphCoderWireDispatcher\b/u);

  for (const name of ["GraphCoderWireMethod", "GraphCoderBridge", "GraphCoderWireParamsByMethod", "GraphCoderWireResultByMethod"]) {
    const definitions = [...sources.entries()].filter(([, source]) => new RegExp("export\\s+(?:type|interface|class)\\s+" + name + "\\b", "u").test(source));
    assert.equal(definitions.length, 1, name + " has duplicated public definitions");
    assert.equal(modulePath(definitions[0][0]), "bridge.ts", name + " moved out of the canonical bridge contract");
  }
  for (const [name, file] of [["GraphCoderUi", "api.ts"], ["BridgeGraphCoderTransport", "bridge.ts"], ["GraphCoderWireDispatcher", "dispatcher.ts"]]) {
    const definitions = [...sources.entries()].filter(([, source]) => new RegExp("export\\s+class\\s+" + name + "\\b", "u").test(source));
    assert.equal(definitions.length, 1, name + " has duplicated domain implementations");
    assert.equal(modulePath(definitions[0][0]), file);
  }
  assert.equal(existsSync(join(packageRoot, "generated")), false, "GraphCoder must consume shared generated contracts rather than minting a private generated schema");
});

test("GraphCoder source has no web, cloud, production-model, or sandbox imports", () => {
  const forbidden = /^(?:node:(?:http|https|net|tls|dns|fs|fs\/promises|worker_threads|vm)|https?:|@(?:aws-sdk|azure|google-cloud|openai|anthropic)\/)/u;
  for (const [path, source] of sources) {
    for (const specifier of importedSpecifiers(source)) {
      assert.equal(forbidden.test(specifier), false, modulePath(path) + " imports forbidden capability " + specifier);
      assert.equal(specifier.startsWith("@acyclic-labs/"), false, modulePath(path) + " reaches another product package directly");
    }
  }
});
