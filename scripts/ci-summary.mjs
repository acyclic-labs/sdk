// Summarizes one qualification lane's timings as Markdown: per-step wall time
// (steps.tsv, written by qualify-ci.sh), the slowest Rust and TypeScript tests
// (JUnit from cargo-nextest and bun test) with a rerun command for each slow
// Rust test, and compiler cache statistics (sccache.json). Prints to stdout
// and appends to $GITHUB_STEP_SUMMARY.
//
// Usage: node scripts/ci-summary.mjs <observability directory> [title]
import { appendFileSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const [directory, title = "Lane timings"] = process.argv.slice(2);
if (!directory) throw new Error("usage: ci-summary.mjs <directory> [title]");
const read = name => {
  const path = join(directory, name);
  return existsSync(path) ? readFileSync(path, "utf8") : "";
};
const seconds = value => `${value.toFixed(value < 10 ? 1 : 0)}s`;
const cell = text => text.replace(/\|/g, "\\|").replace(/\s+/g, " ").trim();
const table = (header, rows) =>
  rows.length === 0
    ? []
    : [`| ${header.join(" | ")} |`, `|${header.map(() => " --- |").join("")}`,
       ...rows.map(row => `| ${row.join(" | ")} |`), ""];
const out = [`## ${title}`, ""];

// steps.tsv holds NUL-terminated "<epoch seconds>\t<command>" records, one per
// top-level command; each command runs until the next starts, and the last
// until now. Commands under a second are folded into one row.
const records = read("steps.tsv").split("\0").filter(Boolean).map(record => {
  const tab = record.indexOf("\t");
  return { start: Number(record.slice(0, tab).replace(",", ".")), command: record.slice(tab + 1) };
});
const steps = records.map((record, index) => ({
  command: record.command,
  time: (records[index + 1]?.start ?? Date.now() / 1000) - record.start,
}));
const shown = steps.filter(step => step.time >= 1);
const rest = steps.filter(step => step.time < 1);
out.push(...table(["Step", "Wall time"], [
  ...shown.map(step => [`\`${cell(step.command).slice(0, 110)}\``, seconds(step.time)]),
  ...(rest.length ? [[`${rest.length} shorter commands`, seconds(rest.reduce((sum, step) => sum + step.time, 0))]] : []),
  ...(steps.length ? [["**Total**", `**${seconds(steps.reduce((sum, step) => sum + step.time, 0))}**`]] : []),
]));

// JUnit: nextest writes one <testsuite> per test binary, bun one per file.
const entity = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" };
const attributes = tag =>
  Object.fromEntries([...tag.matchAll(/(\w+)="([^"]*)"/g)].map(([, key, value]) =>
    [key, value.replace(/&(\w+);/g, (match, name) => entity[name] ?? match)]));
/** @type {Record<string, { name: string, cases: { name: string, time: number }[], failures: number }[]>} */
const suites = { rust: [], typescript: [] };
for (const name of existsSync(directory) ? readdirSync(directory).sort() : []) {
  if (!name.endsWith(".xml")) continue;
  const xml = read(name);
  const kind = /<testsuites [^>]*name="nextest-run"/.test(xml) ? "rust" : "typescript";
  for (const [, open, body = ""] of xml.matchAll(/<testsuite ([^>]*?)(?:\/>|>([\s\S]*?)<\/testsuite>)/g)) {
    const cases = [...body.matchAll(/<testcase ([^>]*?)\/?>/g)].map(([, tag]) => attributes(tag))
      .map(test => ({ name: test.name, time: Number(test.time) || 0 }));
    const failures = (body.match(/<(failure|error)\b/g) ?? []).length;
    suites[kind].push({ name: attributes(open).name, cases, failures });
  }
}
const slowest = (kind, label, count) => {
  const tests = suites[kind].flatMap(suite => suite.cases.map(test => ({ ...test, suite: suite.name })));
  tests.sort((a, b) => b.time - a.time);
  out.push(...table([`Slowest ${label} tests`, "Binary or file", "Time"], tests.slice(0, count)
    .map(test => [`\`${cell(test.name)}\``, `\`${cell(test.suite)}\``, seconds(test.time)])));
  if (kind === "rust" && tests.length) {
    // nextest names each suite after its binary id, `<package>[::<binary>]`.
    out.push("<details><summary>Rerun a slow Rust test alone</summary>", "", "```sh",
      ...tests.slice(0, count).map(test =>
        `cargo nextest run -p ${test.suite.split("::")[0]} --all-features -E 'binary_id(=${test.suite}) & test(=${test.name})'`),
      "```", "", "</details>", "");
  }
  const totals = suites[kind].map(suite => ({
    ...suite, time: suite.cases.reduce((sum, test) => sum + test.time, 0),
  })).sort((a, b) => b.time - a.time);
  out.push(...table([`${label} binary or file`, "Tests", "Failed", "Summed test time"], totals.slice(0, count)
    .map(suite => [`\`${cell(suite.name)}\``, String(suite.cases.length), String(suite.failures), seconds(suite.time)])));
};
slowest("rust", "Rust", 20);
slowest("typescript", "TypeScript", 15);

// sccache --show-stats --stats-format=json.
const cache = (() => {
  try {
    return JSON.parse(read("sccache.json")).stats;
  } catch {
    return null;
  }
})();
if (cache) {
  const sum = counts => Object.values(counts ?? {}).reduce((total, value) => total + Number(value), 0);
  const hits = sum(cache.cache_hits?.counts);
  const misses = sum(cache.cache_misses?.counts);
  out.push(...table(["sccache", "Count"], [
    ["Compile requests", String(cache.compile_requests)],
    ["Cache hits", String(hits)],
    ["Cache misses", String(misses)],
    ["Hit rate", hits + misses ? `${((100 * hits) / (hits + misses)).toFixed(1)}%` : "n/a"],
    ["Not cacheable", String(cache.requests_not_cacheable)],
    ["Cache errors", String(sum(cache.cache_errors?.counts) + Number(cache.cache_read_errors ?? 0))],
    ...Object.entries(cache.not_cached ?? {}).sort(([, a], [, b]) => Number(b) - Number(a))
      .map(([reason, count]) => [`Not cached: \`${cell(reason)}\``, String(count)]),
  ]));
}

const markdown = `${out.join("\n")}\n`;
process.stdout.write(markdown);
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
