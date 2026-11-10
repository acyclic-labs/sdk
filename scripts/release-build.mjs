// Release recipes use the shared artifact cache when the host admits a complete
// builder/input closure. The host also owns the persistent checkout and target.
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export async function releaseBuild({ id, command, args, cwd, env = process.env, output, outputs,
  context = env.SDK_RELEASE_BUILD_CONTEXT, validate }) {
  const started = performance.now();
  let status = 1;
  try {
    await validate?.();
    if (context) {
      const { artifactBuild } = await import(pathToFileURL(
        env.SDK_ARTIFACT_BUILD_MODULE ?? "/mnt/bb/cx/artifact-build.mjs",
      ));
      status = await artifactBuild({ id, command, args, cwd, env, output,
        ...(outputs ? { outputs } : {}), context });
    } else {
      const result = spawnSync(command, args, { cwd, env, stdio: "inherit" });
      if (result.error) throw result.error;
      status = result.status ?? 1;
    }
    return status;
  } finally {
    console.error("RELEASE_BUILD_RESULT " + JSON.stringify({ id,
      status: status === 0 ? "PASS" : "FAIL", seconds: (performance.now() - started) / 1000 }));
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [id, output, separator, command, ...args] = process.argv.slice(2);
  if (!id || !output || separator !== "--" || !command) {
    throw new Error("usage: release-build.mjs ID OUTPUT -- COMMAND [ARG...]");
  }
  process.exitCode = await releaseBuild({ id, output: resolve(output), command, args, cwd: process.cwd() });
}
