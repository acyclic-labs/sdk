#!/usr/bin/env node

// Explicit mock-stage JSON-lines bridge for the installed GraphCoder package.
// This exercises the packaged process boundary and dispatcher while keeping
// fixture evidence separate from the production Harness/local-runtime lane.

import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

function fail(message) {
  throw new Error(`graphcoder-mock-bridge: ${message}`);
}

const packageRoot = process.env.GRAPHCODER_PACKAGE_ROOT;
if (typeof packageRoot !== "string" || packageRoot.trim() === "") fail("GRAPHCODER_PACKAGE_ROOT is required");
const root = packageRoot.trim();
const packageJsonPath = join(root, "package.json");
const resolveExport = createRequire(packageJsonPath);
let mockPath;
let dispatcherPath;
try {
  mockPath = resolveExport.resolve("@acyclic-labs/graphcoder/mock");
  dispatcherPath = resolveExport.resolve("@acyclic-labs/graphcoder/node-dispatcher");
} catch (error) {
  fail(`installed package export could not be resolved: ${error instanceof Error ? error.message : String(error)}`);
}

const [{ createMockTransport }, { runNodeGraphCoderDispatcher }] = await Promise.all([
  import(pathToFileURL(mockPath).href),
  import(pathToFileURL(dispatcherPath).href),
]);
const fixture = process.env.GRAPHCODER_MOCK_FIXTURE;
if (fixture === undefined || fixture.trim() === "") fail("GRAPHCODER_MOCK_FIXTURE is required for an explicit mock stage");
await runNodeGraphCoderDispatcher({ transport: createMockTransport({ fixture }) });
