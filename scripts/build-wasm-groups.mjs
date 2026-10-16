import { spawn } from "node:child_process";

// These groups own distinct Cargo target directories; the seven packages in
// the first group must remain serial because they share the root target.
const groups = [
  { packages: ["actors", "auth", "filesystem", "objects", "machines", "inference", "manifest"], workers: 6 },
  { packages: ["stream"], workers: 4 },
  { packages: ["harness"], workers: 6 },
];

const results = await Promise.allSettled(groups.map(async ({ packages, workers }) => {
  for (const name of packages) {
    await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ["scripts/build-wasm.mjs", name], {
        stdio: "inherit",
        env: { ...process.env, CARGO_BUILD_JOBS: String(workers) },
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
