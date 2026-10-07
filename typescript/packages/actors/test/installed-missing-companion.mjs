#!/usr/bin/env node
/*
 * Run this against a packed and installed Actors package:
 *
 *   node installed-missing-companion.mjs <consumer-directory>
 *
 * The consumer must contain the platform companion listed by the parent
 * package.  The companion is hidden for one child-process import, which
 * verifies the public package falls back to its bundled WASM implementation.
 * The child imports only the installed package, so this exercises packaging
 * and module resolution rather than a source-tree mock of the loader.
 */
import { access, rename } from "node:fs/promises";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const consumer = resolve(process.argv[2] ?? process.cwd());
const companionName = "@acyclic-labs/actors-win32-x64-msvc";
const companion = resolve(consumer, "node_modules", companionName);
const hidden = `${companion}.missing-for-test`;

await access(companion);
await rename(companion, hidden);
try {
  const child = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { ActorId, CodeSha256, CurrentHeadMarker, PositiveU64 } from "@acyclic-labs/actors";
const actor = await ActorId("actor-a");
const digest = await CodeSha256(new Uint8Array(32).fill(1));
const positive = await PositiveU64(1n);
const current = await CurrentHeadMarker(true);
console.log(JSON.stringify({ actor, digestBytes: digest.length, positive: positive.toString(), current }));`,
    ],
    { cwd: consumer, encoding: "utf8" },
  );

  if (child.error) throw child.error;
  if (child.status !== 0) {
    throw new Error(`installed package did not fall back to WASM:\n${child.stderr}`);
  }

  const result = JSON.parse(child.stdout.trim());
  if (result.actor !== "actor-a" || result.digestBytes !== 32 || result.positive !== "1" || result.current !== true) {
    throw new Error(`unexpected fallback result: ${child.stdout}`);
  }
  console.log(JSON.stringify({ consumer, companion: companionName, fallback: "wasm", result }));
} finally {
  await rename(hidden, companion);
}
