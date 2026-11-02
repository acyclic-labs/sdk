import { runCommand } from "./run-command.mjs";

// These groups own distinct Cargo target directories; the seven packages in
// the first group must remain serial because they share the root target.
const groups = [
  { packages: ["actors", "auth", "filesystem", "objects", "machines", "inference", "manifest"], workers: 6 },
  { packages: ["stream"], workers: 4 },
  { packages: ["harness"], workers: 6 },
];

// Shared generators finish before any group can read or replace their outputs.
const generated = await runCommand(process.execPath, ["scripts/generate.mjs"]);
if (generated !== 0) throw new Error(
  `step=generate WASM inputs: exited with ${generated}; recovery: node scripts/generate.mjs`,
);

const results = await Promise.allSettled(groups.map(async ({ packages, workers }) => {
  for (const name of packages) {
    const status = await runCommand(process.execPath, ["scripts/build-wasm.mjs", name], {
      env: { ...process.env, ACYCLIC_WASM_GROUPS_PREPARED: "1", CARGO_BUILD_JOBS: String(workers) },
    });
    if (status !== 0) throw new Error(
      `step=build ${name} WASM: exited with ${status}; recovery: node scripts/build-wasm.mjs ${name}`,
    );
  }
}));
for (const result of results) {
  if (result.status === "rejected") {
    console.error(result.reason);
    process.exitCode = 1;
  }
}
