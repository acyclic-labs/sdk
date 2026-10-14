import { appendFileSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

/** @param {string} matchedKey @param {string[]} snapshots */
export function cacheSaveDecision(matchedKey, snapshots) {
  if (!matchedKey) return { save: true, reason: "no restored cache" };
  if (!snapshots.length) return { save: true, reason: "no compiler snapshot" };
  for (const path of snapshots) {
    let snapshot;
    try {
      snapshot = JSON.parse(readFileSync(path, "utf8"));
    } catch {
      return { save: true, reason: "compiler snapshot unavailable" };
    }
    const stats = snapshot?.stats;
    const counts = stats?.cache_misses?.counts;
    if (!Number.isSafeInteger(stats?.compile_requests) || stats.compile_requests < 0 ||
        !counts || typeof counts !== "object" || Array.isArray(counts) ||
        Object.values(counts).some(value => typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)) {
      return { save: true, reason: "compiler snapshot incomplete" };
    }
    // A restarted daemon reports zero requests even after an earlier build.
    if (stats.compile_requests === 0) return { save: true, reason: "no compiler requests recorded" };
    if (Object.values(counts).some(value => value !== 0)) {
      return { save: true, reason: "compiler cache misses" };
    }
  }
  return { save: false, reason: "restored cache unchanged: zero compiler misses" };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [matchedKey = "", ...snapshots] = process.argv.slice(2);
  const decision = cacheSaveDecision(matchedKey, snapshots);
  console.log(`Compiler cache save=${decision.save}: ${decision.reason}`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `save=${decision.save}\n`);
}
