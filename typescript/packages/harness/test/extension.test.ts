import { expect, test } from "bun:test";
import { validateContract } from "../generated/wasm/acyclic_harness_wasm.js";
import { decodeClientJson, initializeClientQueries } from "../src/client.js";

await initializeClientQueries();

// These golden fixtures use small integers. Never round a newly widened field.
const json = (value: unknown): string => JSON.stringify(value, (_key, part: unknown) => {
  if (typeof part !== "bigint") return part;
  const exact = Number(part);
  if (!Number.isSafeInteger(exact)) throw new RangeError("fixture integer is not exact");
  return exact;
});
const decode = (fixture: string) => decodeClientJson(new TextEncoder().encode(fixture));

test("Rust and TypeScript share the canonical v2 extension record fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-record.json", import.meta.url)).text()).trim();
  const record = validateContract("extension_record", decode(fixture), undefined);
  expect(json(record)).toBe(fixture);
  expect(() => validateContract("extension_record", { ...record, implementation_digest: Array(32).fill(0) }, undefined)).toThrow();
  expect(() => validateContract("extension_record", { ...record, name: "example.\u0000state" }, undefined)).toThrow();
});

test("Rust and TypeScript share the exact extension migration envelope", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-state-migration.json", import.meta.url)).text()).trim();
  const migration = validateContract("extension_state_migration", decode(fixture), undefined);
  expect(json(migration)).toBe(fixture);
  expect(typeof migration.previous.revision).toBe("bigint");
  expect(() => validateContract("extension_state_migration", { ...migration, previous: { ...migration.previous, revision: 0n } }, undefined)).toThrow();
});

test("Rust and TypeScript share the exact extension dependency fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-dependency.json", import.meta.url)).text()).trim();
  const dependency = validateContract("extension_dependency", decode(fixture), undefined);
  expect(json(dependency)).toBe(fixture);
  expect(() => validateContract("extension_dependency", { name: "example.state", version: 0 }, undefined)).toThrow();
});

test("Rust and TypeScript share the ref-only extension configuration fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-configuration.json", import.meta.url)).text()).trim();
  const configuration = validateContract("extension_configuration", decode(fixture), undefined);
  expect(json(configuration)).toBe(fixture);
  expect(() => validateContract("extension_configuration", { ...configuration, schema_digest: Array(32).fill(0) }, undefined)).toThrow();
});

test("Rust and TypeScript share the pinned extension admission fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-admission.json", import.meta.url)).text()).trim();
  const admission = validateContract("extension_admission", decode(fixture), undefined);
  expect(json(admission)).toBe(fixture);
  expect(typeof admission.source.revision).toBe("bigint");
  expect(() => validateContract("extension_admission", { ...admission, source: { ...admission.source, revision: 0n } }, undefined)).toThrow();
});
