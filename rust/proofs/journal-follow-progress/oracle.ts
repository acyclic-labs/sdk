import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initSync, qualifyJournalCancellation, qualifyJournalProgress } from "../../crates/conformance/cloudflare/generated/acyclic_cloudflare_conformance.js";
import { SqliteJournal } from "../../../typescript/packages/harness/src/cloudflare-storage.js";
import { sqlite } from "../../../typescript/packages/harness/test/support/sqlite.js";

const root = mkdtempSync(join(tmpdir(), "acyclic-journal-progress-"));
const database = new Database(join(root, "journal.sqlite"));
const claim = process.argv[2] === "--cancellation" ? "journal-cancellation-progress" : "journal-follow-progress";
try {
  database.run("PRAGMA journal_mode=WAL");
  database.run("PRAGMA synchronous=FULL");
  initSync({ module: readFileSync(new URL("../../crates/conformance/cloudflare/generated/acyclic_cloudflare_conformance_bg.wasm", import.meta.url)) });
  const arguments_ = process.argv.slice(2);
  if (arguments_.length === 1 && arguments_[0] === "--cancellation") {
    const storage = sqlite(database);
    const sync = storage.sync.bind(storage);
    console.log("journal public conditional cancellation START");
    await qualifyJournalCancellation(new SqliteJournal(storage, "cancellation"), () => {
      const count = () => (database.query("SELECT count FROM acyclic_stream_domains WHERE domain = ?").get("cancellation") as { count: number }).count;
      const before = count();
      const entered = Promise.withResolvers<void>();
      const acknowledged = Promise.withResolvers<void>();
      storage.sync = async () => {
        await sync();
        if (count() > before) {
          entered.resolve();
          await acknowledged.promise;
        }
      };
      return [entered.promise, async () => {
        storage.sync = sync;
        acknowledged.resolve();
        await acknowledged.promise;
      }];
    });
    console.log("journal public conditional cancellation PASS");
  } else if (arguments_.length === 0) {
    await qualifyJournalProgress(new SqliteJournal(sqlite(database), "progress"), () => {
      const original = database.query("SELECT configuration, authority, count, bytes FROM acyclic_stream_domains WHERE domain = ?").get("progress") as {
        configuration: Uint8Array; authority: Uint8Array; count: number; bytes: number;
      };
      database.run("DELETE FROM acyclic_stream_domains WHERE domain = ?", "progress");
      return () => database.run("INSERT INTO acyclic_stream_domains (domain, configuration, authority, count, bytes) VALUES (?, ?, ?, ?, ?)",
        "progress", original.configuration, original.authority, original.count, original.bytes);
    });
    console.log("journal public follow retirement PASS");
  } else {
    throw new Error("unsupported selector; use no arguments or --cancellation");
  }
} catch (error) {
  console.error(`STEP_FAILED journal-public-progress: ${error}; recovery: node rust/proofs/qualify.mjs ${claim}`);
  process.exitCode = 1;
} finally {
  database.close();
  rmSync(root, { recursive: true });
}
