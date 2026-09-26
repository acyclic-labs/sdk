import { expect, test } from "bun:test";
import { extensionAdmission, extensionConfiguration, extensionDependency, extensionIdentity, extensionRecord, extensionStateMigration, type ExtensionAdmission, type ExtensionConfiguration, type ExtensionDependency, type ExtensionIdentity, type ExtensionRecord, type ExtensionStateMigration, type ExtensionRegistry, type NativeExtension } from "../src/index.js";

const json = (value: unknown): string => JSON.stringify(value, (_key, part: unknown) =>
  typeof part === "bigint" ? Number(part) : part);

test("Rust and TypeScript share the canonical v2 extension record fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-record.json", import.meta.url)).text()).trim();
  const record = await extensionRecord(JSON.parse(fixture) as ExtensionRecord);
  expect(JSON.stringify(record)).toBe(fixture);
  expect(Object.isFrozen(record.schema_digest)).toBe(true);
  expect(Object.isFrozen(record.content.volume)).toBe(true);
  await expect(extensionRecord({ ...record, implementation_digest: Array(32).fill(0) })).rejects.toThrow();
  await expect(extensionRecord({ ...record, name: "example.\u0000state" })).rejects.toThrow();
});

test("Rust and TypeScript share the exact extension migration envelope", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-state-migration.json", import.meta.url)).text()).trim();
  const migration = await extensionStateMigration(JSON.parse(fixture) as ExtensionStateMigration);
  expect(json(migration)).toBe(fixture);
  expect(typeof migration.previous.revision).toBe("bigint");
  await expect(extensionStateMigration({ ...migration, previous: { ...migration.previous, revision: 0n } })).rejects.toThrow();
});

test("Rust and TypeScript share the exact extension dependency fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-dependency.json", import.meta.url)).text()).trim();
  const dependency = await extensionDependency(JSON.parse(fixture) as ExtensionDependency);
  expect(JSON.stringify(dependency)).toBe(fixture);
  await expect(extensionDependency({ name: "example.state", version: 0 })).rejects.toThrow();
});

test("Rust and TypeScript share the ref-only extension configuration fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-configuration.json", import.meta.url)).text()).trim();
  const configuration = await extensionConfiguration(JSON.parse(fixture) as ExtensionConfiguration);
  expect(JSON.stringify(configuration)).toBe(fixture);
  await expect(extensionConfiguration({ ...configuration, schema_digest: Array(32).fill(0) })).rejects.toThrow();
});

test("Rust and TypeScript share the pinned extension admission fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-admission.json", import.meta.url)).text()).trim();
  const admission = await extensionAdmission(JSON.parse(fixture) as ExtensionAdmission);
  expect(json(admission)).toBe(fixture);
  expect(typeof admission.source.revision).toBe("bigint");
  await expect(extensionAdmission({ ...admission, source: { ...admission.source, revision: 0n } })).rejects.toThrow();
});

test("the TypeScript lifecycle facade keeps executable identity separate from durable dependency state", async () => {
  const record = await extensionRecord(JSON.parse(
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
  expect(Object.isFrozen(identity)).toBe(true);
  await expect(extensionIdentity({ ...identity, digest: Array(32).fill(0) })).rejects.toThrow();
  await expect(extensionIdentity({ ...identity, version: 0 })).rejects.toThrow();
});

test("lifecycle contracts expose host-owned leases and no TypeScript registry implementation", () => {
  const implementation: NativeExtension = {
    identity: () => ({ name: "example.state", version: 1, digest: Array(32).fill(1) }),
    dependencies: () => [],
  };
  const registry: ExtensionRegistry = {
    install: () => undefined,
    register: () => undefined,
    disable: () => undefined,
    pin: () => ({ identity: implementation.identity, dispose: () => undefined }),
    pinExact: () => ({ identity: implementation.identity, dispose: () => undefined }),
    remove: () => undefined,
    contains: () => true,
    accepting: () => true,
    current: () => implementation.identity(),
    exact: () => implementation.identity(),
  };
  expect(registry.accepting("example.state")).toBe(true);
  expect(registry.pin("example.state").identity()).toEqual(implementation.identity());
});
