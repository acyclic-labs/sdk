import { expect, test } from "bun:test";
import { NativeContracts, extensionIdentity, type ExtensionAdmission, type ExtensionConfiguration, type ExtensionDependency, type ExtensionRecord, type ExtensionStateMigration } from "../src/index.js";

const contracts = await NativeContracts.create();

const json = (value: unknown): string => JSON.stringify(value, (_key, part: unknown) =>
  typeof part === "bigint" ? Number(part) : part);

test("Rust and TypeScript share the canonical v2 extension record fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-record.json", import.meta.url)).text()).trim();
  const record = contracts.validate("extension_record", JSON.parse(fixture) as ExtensionRecord);
  expect(JSON.stringify(record)).toBe(fixture);
  expect(() => contracts.validate("extension_record", { ...record, implementation_digest: Array(32).fill(0) })).toThrow();
  expect(() => contracts.validate("extension_record", { ...record, name: "example.\u0000state" })).toThrow();
});

test("Rust and TypeScript share the exact extension migration envelope", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-state-migration.json", import.meta.url)).text()).trim();
  const migration = contracts.validate("extension_state_migration", JSON.parse(fixture) as ExtensionStateMigration);
  expect(json(migration)).toBe(fixture);
  expect(typeof migration.previous.revision).toBe("bigint");
  expect(() => contracts.validate("extension_state_migration", { ...migration, previous: { ...migration.previous, revision: 0n } })).toThrow();
});

test("Rust and TypeScript share the exact extension dependency fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-dependency.json", import.meta.url)).text()).trim();
  const dependency = contracts.validate("extension_dependency", JSON.parse(fixture) as ExtensionDependency);
  expect(JSON.stringify(dependency)).toBe(fixture);
  expect(() => contracts.validate("extension_dependency", { name: "example.state", version: 0 })).toThrow();
});

test("Rust and TypeScript share the ref-only extension configuration fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-configuration.json", import.meta.url)).text()).trim();
  const configuration = contracts.validate("extension_configuration", JSON.parse(fixture) as ExtensionConfiguration);
  expect(JSON.stringify(configuration)).toBe(fixture);
  expect(() => contracts.validate("extension_configuration", { ...configuration, schema_digest: Array(32).fill(0) })).toThrow();
});

test("Rust and TypeScript share the pinned extension admission fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-admission.json", import.meta.url)).text()).trim();
  const admission = contracts.validate("extension_admission", JSON.parse(fixture) as ExtensionAdmission);
  expect(json(admission)).toBe(fixture);
  expect(typeof admission.source.revision).toBe("bigint");
  expect(() => contracts.validate("extension_admission", { ...admission, source: { ...admission.source, revision: 0n } })).toThrow();
});

test("the TypeScript lifecycle facade keeps executable identity separate from durable dependency state", async () => {
  const record = contracts.validate("extension_record", JSON.parse(
    (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-record.json", import.meta.url)).text()).trim(),
  ) as ExtensionRecord);
  const identity = await extensionIdentity({
    name: record.name,
    version: record.version,
    digest: record.implementation_digest,
  });
  expect(identity).toEqual({
    name: record.name,
    version: record.version,
    digest: record.implementation_digest,
  });
  await expect(extensionIdentity({ ...identity, digest: Array(32).fill(0) })).rejects.toThrow();
  await expect(extensionIdentity({ ...identity, version: 0 })).rejects.toThrow();
});
