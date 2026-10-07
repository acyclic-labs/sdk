import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { readFile, writeFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const ADMISSION_ERROR = "expected a finite integer in the u32 range";
const WASM_NUMBER_ERROR = "expected a JavaScript number";
const EDGE_VALUES = [
  ["zero", 0],
  ["one", 1],
  ["u32-max", 4_294_967_295],
];
const EDGE_OUTCOMES = new Map([
  ["zero", "downstream:workspace join exceeds its configured bound"],
  ["one", "downstream:workspace is not a fork"],
  ["u32-max", "downstream:workspace is not a fork"],
]);
const REJECT_VALUES = [
  ["negative", -1],
  ["fractional-half", 0.5],
  ["fractional-one-and-half", 1.5],
  ["u32-overflow", 4_294_967_296],
  ["nan", Number.NaN],
  ["infinity", Number.POSITIVE_INFINITY],
  ["negative-infinity", Number.NEGATIVE_INFINITY],
  ["null", null],
  ["string-one", "1"],
  ["true", true],
  ["false", false],
  ["undefined", undefined],
  ["bigint-one", 1n],
  ["boxed-one", new Number(1)],
  ["symbol-one", Symbol("1")],
];

const args = process.argv.slice(2);
const argument = name => {
  const index = args.indexOf(name);
  if (index < 0 || args[index + 1] === undefined) throw new Error(`missing ${name}`);
  return args[index + 1];
};
const packageRoot = resolve(argument("--package-root"));
const nativeBinding = resolve(argument("--native-binding"));
const sourceCommit = argument("--source-commit");
const archivePath = resolve(argument("--archive"));
const receiptPath = resolve(argument("--receipt"));
if (!/^[0-9a-f]{40}$/i.test(sourceCommit)) {
  throw new Error("--source-commit must be the full 40-character checked-out commit");
}

const digest = async path => {
  const hash = createHash("sha256");
  hash.update(await readFile(path));
  return hash.digest("hex");
};
const describe = value => {
  if (typeof value === "symbol") return "Symbol(1)";
  if (typeof value === "bigint") return `${value}n`;
  if (value instanceof Number) return "new Number(1)";
  if (value === undefined) return "undefined";
  if (Number.isNaN(value)) return "NaN";
  if (value === Number.POSITIVE_INFINITY) return "Infinity";
  if (value === Number.NEGATIVE_INFINITY) return "-Infinity";
  return JSON.stringify(value);
};

const likelyDownstream = message =>
  message.includes("workspace is not a fork") ||
  message.includes("workspace join exceeds its configured bound");
const classify = (error, runtime, value) => {
  const message = String(error?.message ?? error);
  if (message.includes(ADMISSION_ERROR)) return "boundary_rejected";
  if (runtime === "wasm" && message === WASM_NUMBER_ERROR) return "boundary_rejected";
  // N-API rejects non-number values in its f64 decoder before Rust's shared
  // helper runs. Preserve that native boundary result while still detecting
  // an accidental ToNumber conversion that reaches Rust policy.
  if (runtime === "napi" && typeof value !== "number" && !likelyDownstream(message)) {
    return "boundary_rejected";
  }
  return `downstream:${message}`;
};
const values = [...EDGE_VALUES, ...REJECT_VALUES];

async function qualifyWasm() {
  const jsPath = join(packageRoot, "generated", "wasm", "acyclic_fs_wasm.js");
  const wasmPath = join(packageRoot, "generated", "wasm", "acyclic_fs_wasm_bg.wasm");
  const module = await import(pathToFileURL(jsPath).href);
  await module.default({ module_or_path: await readFile(wasmPath) });
  const result = [];
  for (const [label, value] of values) {
    const fs = module.openMemoryFs({
      maximumObjectBytes: 1024 * 1024,
      maximumMemoryBytes: 64 * 1024 * 1024,
      objectCache: { maximumEntries: 8, maximumBytes: 1024, maximumInFlight: 2, maximumWaitersPerObject: 2 },
    });
    let outcome;
    try {
      const workspace = await fs.createWorkspace(`admission-${label}`);
      try {
        await workspace.liveRebase(null, value, 1, 1);
        outcome = "accepted";
      } catch (error) {
        outcome = classify(error, "wasm", value);
      }
    } finally {
      fs.close?.();
    }
    result.push({ label, value: describe(value), outcome });
  }
  return {
    runtime: "wasm",
    artifacts: {
      js: { path: "generated/wasm/acyclic_fs_wasm.js", sha256: await digest(jsPath) },
      wasm: { path: "generated/wasm/acyclic_fs_wasm_bg.wasm", sha256: await digest(wasmPath) },
    },
    results: result,
  };
}

async function qualifyNative() {
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-fs-admission-"));
  try {
    const binding = createRequire(import.meta.url)(nativeBinding);
    const fs = await binding.NativeFs.open(join(temporary, "engine"), {
      maximumEntries: 64,
      maximumBytes: 1024n * 1024n,
      maximumInFlight: 2,
      maximumWaitersPerObject: 2,
    });
    const result = [];
    try {
      for (const [label, value] of values) {
        const workspace = await fs.createWorkspace(`admission-${label}`);
        let outcome;
        try {
          await workspace.liveRebase(null, value, 1, 1);
          outcome = "accepted";
        } catch (error) {
          outcome = classify(error, "napi", value);
        }
        result.push({ label, value: describe(value), outcome });
      }
    } finally {
      fs.cancel();
    }
    return {
      runtime: "napi",
      artifacts: { binding: { path: basename(nativeBinding), sha256: await digest(nativeBinding) } },
      results: result,
    };
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

const runtimes = [await qualifyWasm(), await qualifyNative()];
const failures = [];
for (const runtime of runtimes) {
  for (const row of runtime.results) {
    const isReject = row.label !== "zero" && row.label !== "one" && row.label !== "u32-max";
    if (isReject && row.outcome !== "boundary_rejected") {
      failures.push(`${runtime.runtime} accepted ${row.value}: ${row.outcome}`);
    }
    if (!isReject && row.outcome !== EDGE_OUTCOMES.get(row.label)) {
      failures.push(`${runtime.runtime} returned unexpected result for valid edge ${row.value}: ${row.outcome}`);
    }
  }
}
if (failures.length > 0) {
  throw new Error(`filesystem admission failed:\n${failures.join("\n")}\nfull matrix: ${JSON.stringify(runtimes)}`);
}
const matrix = JSON.stringify(runtimes);
const receipt = {
  schema: 1,
  source_commit: sourceCommit,
  package_archive: { path: basename(archivePath), sha256: await digest(archivePath) },
  matrix_sha256: createHash("sha256").update(matrix).digest("hex"),
  runtimes,
};
await writeFile(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify(receipt, null, 2));
