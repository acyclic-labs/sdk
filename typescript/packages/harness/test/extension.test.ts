import { expect, test } from "bun:test";
import { extensionRecord, type ExtensionRecord } from "../src/index.js";

test("Rust and TypeScript share the canonical v2 extension record fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/extension-record.json", import.meta.url)).text()).trim();
  const record = await extensionRecord(JSON.parse(fixture) as ExtensionRecord);
  expect(JSON.stringify(record)).toBe(fixture);
  expect(Object.isFrozen(record.schema_digest)).toBe(true);
  expect(Object.isFrozen(record.content.volume)).toBe(true);
  await expect(extensionRecord({ ...record, implementation_digest: Array(32).fill(0) })).rejects.toThrow();
  await expect(extensionRecord({ ...record, name: "example.\u0000state" })).rejects.toThrow();
});
