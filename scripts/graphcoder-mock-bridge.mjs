#!/usr/bin/env node

// Explicit mock-stage JSON-lines bridge for the installed GraphCoder package.
// This exercises the packaged process boundary and dispatcher while keeping
// fixture evidence separate from the production Harness/local-runtime lane.

import { createRequire } from "node:module";
import { lstatSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

function fail(message) {
  throw new Error(`graphcoder-mock-bridge: ${message}`);
}

const packageRoot = process.env.GRAPHCODER_PACKAGE_ROOT;
if (typeof packageRoot !== "string" || packageRoot.trim() === "") fail("GRAPHCODER_PACKAGE_ROOT is required");
const root = packageRoot.trim();
const packageJsonPath = join(root, "package.json");
const packageMetadata = lstatSync(packageJsonPath, { throwIfNoEntry: false });
if (!packageMetadata?.isFile() || packageMetadata.isSymbolicLink()) fail(`installed package.json must be a regular file: ${packageJsonPath}`);
const resolveExport = createRequire(packageJsonPath);
let mockPath;
let dispatcherPath;
let apiPath;
try {
  mockPath = resolveExport.resolve("@acyclic-labs/graphcoder/mock");
  dispatcherPath = resolveExport.resolve("@acyclic-labs/graphcoder/node-dispatcher");
  apiPath = resolveExport.resolve("@acyclic-labs/graphcoder");
} catch (error) {
  fail(`installed package export could not be resolved: ${error instanceof Error ? error.message : String(error)}`);
}

const [{ createMockTransport }, { runNodeGraphCoderDispatcher }, { GraphCoderError }] = await Promise.all([
  import(pathToFileURL(mockPath).href),
  import(pathToFileURL(dispatcherPath).href),
  import(pathToFileURL(apiPath).href),
]);
const fixture = process.env.GRAPHCODER_MOCK_FIXTURE;
if (fixture === undefined || fixture.trim() === "") fail("GRAPHCODER_MOCK_FIXTURE is required for an explicit mock stage");
const selectedFixture = fixture.trim();

// Keep the fixture identity explicit at the transport boundary too. The
// installed production entrypoint intentionally has no fixture flag, so this
// wrapper records the bridge's configured fixture in every start admission and
// rejects a caller that attempts to select a different one.
const fixtureTransport = createMockTransport({ fixture: selectedFixture });
const transport = new Proxy(fixtureTransport, {
  get(target, property, receiver) {
    if (property === "startSession") {
      return input => {
        if (input.modelFixture !== undefined && input.modelFixture !== selectedFixture) {
          return Promise.reject(new GraphCoderError("unsupported", `mock fixture ${input.modelFixture} does not match configured fixture ${selectedFixture}`));
        }
        return target.startSession({ ...input, modelFixture: selectedFixture });
      };
    }
    const value = Reflect.get(target, property, target);
    return typeof value === "function" ? value.bind(target) : value;
  },
});
await runNodeGraphCoderDispatcher({ transport });
