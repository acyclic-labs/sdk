import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Use the exact runtime installed by the pinned Miniflare toolchain, directly.
// Its internal Node proxy otherwise chooses an unconfigurable ephemeral port:
// https://github.com/cloudflare/workers-sdk/blob/miniflare%404.20260730.0/packages/miniflare/src/index.ts#L1770
const config = fileURLToPath(new URL("./workerd.capnp", import.meta.url));
let workerdPath;
let persistence;
let runtime;
let exited;

async function start() {
  runtime = spawn(workerdPath, ["serve", config, "--experimental", "--control-fd=3",
    `--directory-path=data=${persistence}`], {
    stdio: ["ignore", "inherit", "inherit", "pipe"], windowsHide: true,
    env: { ...process.env, TZ: "UTC" },
  });
  let launchError;
  runtime.once("error", error => { launchError = String(error); });
  exited = new Promise(resolve => runtime.once("close", (code, signal) => resolve({ code, signal, launchError })));
  const control = createInterface(runtime.stdio[3]);
  let timer;
  try {
    await Promise.race([
      new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new Error("workerd startup exceeded 10 seconds")), 10_000);
        control.on("line", line => {
          try {
            const message = JSON.parse(line);
            if (message.event === "listen" && message.socket === "http" && message.port === 15420) resolve();
          } catch (error) { reject(error); }
        });
      }),
      exited.then(result => { throw new Error(`workerd startup exited: ${JSON.stringify(result)}`); }),
    ]);
  } finally { clearTimeout(timer); control.close(); }
}

async function stop() {
  if (!runtime) return;
  const stopped = runtime;
  runtime = undefined;
  if (stopped.exitCode === null && stopped.signalCode === null) stopped.kill("SIGTERM");
  const timer = setTimeout(() => stopped.kill("SIGKILL"), 5000);
  try { await exited; } finally { clearTimeout(timer); }
}

async function main() {
  try {
    const toolchain = createRequire(import.meta.resolve("miniflare"));
    ({ default: workerdPath } = toolchain("workerd"));
    persistence = await mkdtemp(join(tmpdir(), "acyclic-do-conformance-"));
    await start();
    const response = await fetch("http://127.0.0.1:15420/");
    const output = await response.text();
    if (!response.ok) throw new Error(`HTTP ${response.status}: ${output}`);
    console.log(`workerd SQLite: ${output}`);
    const deadline = await fetch("http://127.0.0.1:15420/deadline");
    const deadlineOutput = await deadline.text();
    if (!deadline.ok) throw new Error(`deadline HTTP ${deadline.status}: ${deadlineOutput}`);
    console.log(`workerd review: ${deadlineOutput}`);
    const guest = await fetch("http://127.0.0.1:15420/guest");
    const guestOutput = await guest.text();
    if (!guest.ok) throw new Error(`guest HTTP ${guest.status}: ${guestOutput}`);
    console.log("workerd guest: exact create/send retries committed one conversation record");
    const timeout = await fetch("http://127.0.0.1:15420/timeout");
    const timeoutOutput = await timeout.text();
    if (!timeout.ok) throw new Error(`guest timeout HTTP ${timeout.status}: ${timeoutOutput}`);
    console.log(`workerd guest: ${timeoutOutput}`);
    const cancellation = await fetch("http://127.0.0.1:15420/cancel");
    const cancellationOutput = await cancellation.text();
    if (!cancellation.ok) throw new Error(`guest cancel HTTP ${cancellation.status}: ${cancellationOutput}`);
    console.log(`workerd guest: ${cancellationOutput}`);
    const review = await fetch("http://127.0.0.1:15420/review");
    const reviewOutput = await review.text();
    if (!review.ok) throw new Error(`review regression HTTP ${review.status}: ${reviewOutput}`);
    console.log(`workerd review: ${reviewOutput}`);
    const placement = await fetch("http://127.0.0.1:15420/placement");
    const placementOutput = await placement.text();
    if (!placement.ok) throw new Error(`placement HTTP ${placement.status}: ${placementOutput}`);
    console.log(`workerd review: ${placementOutput}`);
    const interrupted = await fetch("http://127.0.0.1:15420/creation-interrupted");
    const interruptedOutput = await interrupted.text();
    if (!interrupted.ok) throw new Error(`creation interruption HTTP ${interrupted.status}: ${interruptedOutput}`);
    console.log(`workerd review: ${interruptedOutput}`);
    await stop();
    await start();
    const creation = await fetch("http://127.0.0.1:15420/creation-replay");
    const creationOutput = await creation.text();
    if (!creation.ok) throw new Error(`creation restart HTTP ${creation.status}: ${creationOutput}`);
    console.log(`workerd review: ${creationOutput}`);
    const deadlineReplay = await fetch("http://127.0.0.1:15420/deadline-replay");
    const deadlineReplayOutput = await deadlineReplay.text();
    if (!deadlineReplay.ok) throw new Error(`deadline restart HTTP ${deadlineReplay.status}: ${deadlineReplayOutput}`);
    console.log(`workerd review: ${deadlineReplayOutput}`);
    const replay = await fetch("http://127.0.0.1:15420/guest");
    const replayOutput = await replay.text();
    if (!replay.ok || replayOutput !== guestOutput) {
      throw new Error(`guest restart differs: HTTP ${replay.status}: ${replayOutput}`);
    }
    console.log("workerd guest: SQLite survived a full emulator restart; exact create/send retries retained one turn");
    const adopted = await fetch(`http://127.0.0.1:15420/adoption-replay?path=${encodeURIComponent(JSON.parse(reviewOutput).child)}`);
    const adoptedOutput = await adopted.text();
    if (!adopted.ok) throw new Error(`adoption restart HTTP ${adopted.status}: ${adoptedOutput}`);
    console.log(`workerd review: ${adoptedOutput}`);
    const placed = await fetch("http://127.0.0.1:15420/placement-replay");
    const placedOutput = await placed.text();
    if (!placed.ok) throw new Error(`placement restart HTTP ${placed.status}: ${placedOutput}`);
    console.log(`workerd review: ${placedOutput}`);
  } finally {
    await stop();
    if (persistence) await rm(persistence, { recursive: true, force: true });
  }
}

await main().catch(error => {
  console.error(`STEP workerd-conformance failed: ${error}; recovery: bash scripts/qualify-cloudflare.sh`);
  console.error(error);
  process.exitCode = 1;
});
