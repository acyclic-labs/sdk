import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

type Family = "stream" | "objects" | "machines" | "inference";
interface VectorManifest<FamilyName extends Family> { readonly family: FamilyName; readonly cases: readonly string[] }
const ownership = { stream: "@acyclic-labs/stream", objects: "@acyclic-labs/objects", machines: "@acyclic-labs/machines", inference: "@acyclic-labs/inference" } as const satisfies Readonly<Record<Family, string>>;

test("every immutable customer vector has one TypeScript semantic owner", async () => {
  for (const family of Object.keys(ownership) as Family[]) {
    const path = resolve(import.meta.dir, "../../../../conformance/vectors", `${family}.json`);
    const source = JSON.parse(await readFile(path, "utf8")) as { readonly cases?: unknown };
    if (!Array.isArray(source.cases) || !source.cases.every(value => typeof value === "string" && value.length > 0)) throw new TypeError(`${family} vector has invalid cases`);
    const manifest = { family, cases: source.cases } as VectorManifest<typeof family>;
    expect(new Set(manifest.cases).size).toBe(manifest.cases.length);
    expect(ownership[manifest.family]).toStartWith("@acyclic-labs/");
  }
});
