#!/usr/bin/env node

// Explicit mock-stage JSON-lines bridge for the installed GraphCoder package.
// This exercises the packaged process boundary and dispatcher while keeping
// fixture evidence separate from the production Harness/local-runtime lane.

import { existsSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

function fail(message) {
  throw new Error(`graphcoder-mock-bridge: ${message}`);
}

const packageRoot = process.env.GRAPHCODER_PACKAGE_ROOT;
if (typeof packageRoot !== "string" || packageRoot.trim() === "") fail("GRAPHCODER_PACKAGE_ROOT is required");
const root = packageRoot.trim();
const mockPath = join(root, "dist", "mock.js");
const dispatcherPath = join(root, "dist", "node-dispatcher.js");
for (const path of [mockPath, dispatcherPath]) if (!existsSync(path)) fail(`installed package module is missing: ${path}`);

const [{ createMockTransport }, { runNodeGraphCoderDispatcher }] = await Promise.all([
  import(pathToFileURL(mockPath).href),
  import(pathToFileURL(dispatcherPath).href),
]);
const fixture = process.env.GRAPHCODER_MOCK_FIXTURE;
if (fixture === undefined || fixture.trim() === "") fail("GRAPHCODER_MOCK_FIXTURE is required for an explicit mock stage");
await runNodeGraphCoderDispatcher({ transport: createMockTransport({ fixture }) });
