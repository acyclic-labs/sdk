import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

test("Rust credential admission is projected into HTTP facades", async () => {
  const [actorsRust, workersRust, emitter, actorsMetadata, workersMetadata] = await Promise.all([
    source("rust/crates/actors/src/http.rs"),
    source("rust/crates/workers/src/http.rs"),
    source("rust/crates/sdk-typescript/src/main.rs"),
    source("typescript/packages/actors/src/generated-client.ts"),
    source("typescript/packages/workers/src/generated-client.ts"),
  ]);

  for (const rust of [actorsRust, workersRust]) {
    expect(rust).toContain("credential::validate(BEARER_NO_CRLF, token)");
  }
  expect(emitter).toContain('auth: "bearer".to_owned()');
  expect(emitter).toContain("credential_policy");
  for (const metadata of [actorsMetadata, workersMetadata]) {
    expect(metadata).toContain('auth: "bearer"');
    expect(metadata).toContain('credentialPolicy: "bearer-no-crlf"');
    expect(metadata).toContain("validateRustOwnedCredential");
  }

  const [{ HttpActorsClient }, { HttpWorkersClient }] = await Promise.all([
    import(pathToFileURL(join(root, "typescript/packages/actors/src/http.ts")).href),
    import(pathToFileURL(join(root, "typescript/packages/workers/src/http.ts")).href),
  ]);
  const forgedToken = "accepted-before-rust-policy\r\nX-Injected: yes";
  expect(() => new HttpActorsClient({ endpoint: "https://actors.example.test/", token: forgedToken })).toThrow(TypeError);
  expect(() => new HttpWorkersClient({ endpoint: "https://workers.example.test/", token: forgedToken })).toThrow(TypeError);
});
