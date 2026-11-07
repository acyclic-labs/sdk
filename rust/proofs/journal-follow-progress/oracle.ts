import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initSync, qualifyJournalProgress } from "../../crates/conformance/cloudflare/generated/acyclic_cloudflare_conformance.js";
import { SqliteJournal } from "../../../typescript/packages/harness/src/cloudflare-storage.js";
import { sqlite } from "../../../typescript/packages/harness/test/support/sqlite.js";

const root = mkdtempSync(join(tmpdir(), "acyclic-journal-progress-"));
const database = new Database(join(root, "journal.sqlite"));
try {
  database.run("PRAGMA journal_mode=WAL");
  database.run("PRAGMA synchronous=FULL");
  initSync({ module: readFileSync(new URL("../../crates/conformance/cloudflare/generated/acyclic_cloudflare_conformance_bg.wasm", import.meta.url)) });
  await qualifyJournalProgress(new SqliteJournal(sqlite(database), "progress"), () => {
    const original = database.query("SELECT configuration, authority, count, bytes FROM acyclic_stream_domains WHERE domain = ?").get("progress") as {
      configuration: Uint8Array; authority: Uint8Array; count: number; bytes: number;
    };
    database.run("DELETE FROM acyclic_stream_domains WHERE domain = ?", "progress");
    return () => database.run("INSERT INTO acyclic_stream_domains (domain, configuration, authority, count, bytes) VALUES (?, ?, ?, ?, ?)",
      "progress", original.configuration, original.authority, original.count, original.bytes);
  });
  console.log("journal public follow retirement PASS");
} catch (error) {
  console.error(`STEP_FAILED journal-public-progress: ${error}; recovery: node rust/proofs/qualify.mjs journal-follow-progress`);
  process.exitCode = 1;
} finally {
  database.close();
  rmSync(root, { recursive: true });
}
