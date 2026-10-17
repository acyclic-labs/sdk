import { spawn, spawnSync } from "node:child_process";

// These groups own distinct Cargo target directories; the seven packages in
// the first group must remain serial because they share the root target.
const groups = [
  { packages: ["actors", "auth", "filesystem", "objects", "machines", "inference", "manifest"], workers: 6 },
  { packages: ["stream"], workers: 4 },
  { packages: ["harness"], workers: 6 },
];

// Shared generators finish before any group can read or replace their outputs.
const generated = spawnSync(process.execPath, ["scripts/generate.mjs"], { stdio: "inherit" });
if (generated.error) throw generated.error;
if (generated.status !== 0) throw new Error(`WASM generation exited with ${generated.status}`);

const results = await Promise.allSettled(groups.map(async ({ packages, workers }) => {
  for (const name of packages) {
    await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ["scripts/build-wasm.mjs", name], {
        stdio: "inherit",
        env: { ...process.env, ACYCLIC_WASM_GROUPS_PREPARED: "1", CARGO_BUILD_JOBS: String(workers) },
      });
      child.on("error", reject);
      child.on("close", status => status === 0 ? resolve(undefined) : reject(new Error(`${name} WASM build exited with ${status}`)));
    });
  }
}));
for (const result of results) {
  if (result.status === "rejected") {
    console.error(result.reason);
    process.exitCode = 1;
  }
}
