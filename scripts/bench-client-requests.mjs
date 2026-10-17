import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const cargoArgs = [
  "test", "--locked", "-p", "acyclic-actors", "-p", "acyclic-objects", "-p", "acyclic-stream",
  "--lib", "--features", "acyclic-objects/http,acyclic-stream/http",
  "request_path_measure", "--", "--ignored", "--nocapture", "--test-threads=1",
];

const probes = [
  ["actors admitted request conversion", "consume"],
  ["objects frame conversion", "reuse"],
  ["stream append JSON", "direct"],
];

/** @param {string} output */
export function measurements(output) {
  return probes.map(([name, option]) => {
    const samples = [false, true].map(enabled => {
      const pattern = new RegExp(`^${name} ${option}=${enabled} iterations=(\\d+) bytes=(\\d+) elapsed_us=(\\d+)$`, "gm");
      const matches = [...output.matchAll(pattern)];
      if (matches.length !== 1) throw new Error(`expected one ${name} ${option}=${enabled} measurement`);
      const [, iterations, bytes, elapsed] = matches[0];
      if (Number(iterations) <= 0 || Number(bytes) <= 0) throw new Error(`invalid ${name} workload`);
      return { iterations: Number(iterations), bytes: Number(bytes), elapsed_us: Number(elapsed) };
    });
    const [before, after] = samples;
    if (before.iterations !== after.iterations || before.bytes !== after.bytes) {
      throw new Error(`mismatched ${name} workloads`);
    }
    return { name, before, after };
  });
}

/** @param {typeof spawnSync} [run] */
export function benchClientRequests(run = spawnSync) {
  const result = run("cargo", cargoArgs, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  process.stdout.write(result.stdout ?? "");
  process.stderr.write(result.stderr ?? "");
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`request-path measurements failed: ${result.signal ?? result.status}`);
  return measurements(`${result.stdout}\n${result.stderr}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  if (args.length && (args.length !== 2 || args[0] !== "--output")) {
    throw new Error("usage: node scripts/bench-client-requests.mjs [--output report.json]");
  }
  const report = { profile: "test", measurements: benchClientRequests() };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (args[1]) writeFileSync(args[1], json);
  else process.stdout.write(json);
}
