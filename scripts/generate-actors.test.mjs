import assert from "node:assert/strict";
import { cp, mkdtemp, rm } from "node:fs/promises";
import test from "node:test";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { assertTreeEqual } from "./generate-actors.mjs";

const root = join(fileURLToPath(new URL("..", import.meta.url)));
const semantic = join(root, "typescript/packages/actors/src/generated");

test("Actors generation detects a missing Rust-owned semantic output", async () => {
  const temporary = await mkdtemp(join(root, ".tmp-actors-drift-test-"));
  try {
    await cp(semantic, temporary, { recursive: true });
    await rm(join(temporary, "semantic/actors/CurrentHeadMarker.ts"));
    await assert.rejects(
      () => assertTreeEqual(semantic, temporary, "Actors Rust TypeScript generation"),
      /file set drift/,
    );
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
