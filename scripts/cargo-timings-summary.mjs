import { appendFileSync, copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export function readCargoTimings(html) {
  const array = name => {
    const match = html.match(new RegExp(`const ${name} = (\\[[\\s\\S]*?\\]);`));
    if (!match) throw new Error(`Cargo timings missing ${name}`);
    return JSON.parse(match[1]);
  };
  const units = array("UNIT_DATA");
  if (!Array.isArray(units) || units.some(unit => typeof unit.name !== "string"
    || typeof unit.target !== "string" || !Number.isFinite(unit.duration) || unit.duration < 0
    || !Number.isFinite(unit.start) || unit.start < 0)) throw new Error("invalid Cargo timing units");
  return { units, concurrency: array("CONCURRENCY_DATA"), cpuUsage: array("CPU_USAGE") };
}

const cell = text => String(text).replaceAll("|", "\\|").replace(/\s+/g, " ").trim();
const seconds = value => value === null ? "not reported" : `${value.toFixed(2)}s`;
const phase = (unit, name) => {
  const entry = unit.sections?.find(([label]) => label === name)?.[1];
  return entry && Number.isFinite(entry.start) && Number.isFinite(entry.end)
    && entry.end >= entry.start ? entry.end - entry.start : null;
};

export function summarizeCargoTimings(output, builds) {
  mkdirSync(output, { recursive: true });
  const reports = [];
  const errors = [];
  const lines = ["## Native Cargo unit timings", "",
    "Unit durations are elapsed wall time, not CPU-seconds. Stable Cargo may omit binary phase boundaries; a final binary unit includes compilation/codegen/link, not linker-only time.", ""];
  for (const [name, target] of builds) {
    if (!/^[a-z]+$/.test(name)) throw new Error("invalid build label");
    const path = join(target, "cargo-timings", "cargo-timing.html");
    if (!existsSync(path)) continue;
    copyFileSync(path, join(output, `${name}-cargo-timing.html`));
    try {
      const data = readCargoTimings(readFileSync(path, "utf8"));
      const slowest = [...data.units].sort((a, b) => b.duration - a.duration).slice(0, 5);
      const finalBinaryUnits = data.units.filter(unit => unit.name === "acyclic-plugin" && /\bbin\b/.test(unit.target));
      const report = { build: name, runId: process.env.GITHUB_RUN_ID ?? null,
        source: process.env.SDK_SOURCE_SHA ?? null, ...data, slowest, finalBinaryUnits };
      writeFileSync(join(output, `${name}-cargo-timings.json`), JSON.stringify(report, null, 2) + "\n");
      reports.push(report);
      lines.push(`### ${name}`, "", "| Unit | Total | Reported codegen | Reported link |",
        "| --- | ---: | ---: | ---: |",
        ...slowest.map(unit => `| ${cell(unit.name + unit.target)} | ${seconds(unit.duration)} | ${seconds(phase(unit, "codegen"))} | ${seconds(phase(unit, "link"))} |`), "",
        ...finalBinaryUnits.map(unit => `Final plugin binary ${cell(unit.target)}: combined compilation/codegen/link ${seconds(unit.duration)}; reported codegen ${seconds(phase(unit, "codegen"))}; reported link ${seconds(phase(unit, "link"))}.`), "");
    } catch (error) {
      errors.push(`${name}: ${error.message}`);
      lines.push(`### ${name}`, "", `Report retained, summary failed: ${cell(error.message)}`, "");
    }
  }
  const markdown = lines.join("\n") + "\n";
  process.stdout.write(markdown);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
  if (errors.length) throw new Error(errors.join("\n"));
  if (reports.length === 0) throw new Error("no Cargo timing reports were produced");
  return reports;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [output, ...inputs] = process.argv.slice(2);
  if (!output || inputs.length === 0) throw new Error("usage: cargo-timings-summary.mjs <output> <label=target-directory>...");
  summarizeCargoTimings(output, inputs.map(input => {
    const index = input.indexOf("=");
    if (index <= 0 || index === input.length - 1) throw new Error("invalid build directory");
    return [input.slice(0, index), input.slice(index + 1)];
  }));
}
