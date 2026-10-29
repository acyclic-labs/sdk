import { watch } from "node:fs";
import { open } from "node:fs/promises";
import { dirname } from "node:path";
import { StringDecoder } from "node:string_decoder";

// Subscribe before taking the snapshot. Every later read is driven by a file
// notification; process exit cancels the subscription without a readiness loop.
export async function waitForHook(path, match, closed) {
  let found, failed, file, settled = false;
  const ready = new Promise((resolve, reject) => { found = resolve; failed = reject; });
  const watcher = watch(dirname(path));
  const buffer = Buffer.allocUnsafe(64 * 1024), decoder = new StringDecoder("utf8");
  let fragments = [];
  let reads = Promise.resolve();
  const scan = () => {
    reads = reads.then(async () => {
      if (settled) return;
      if (!file) {
        try { file = await open(path, "r"); }
        catch (error) { if (error.code === "ENOENT") return; throw error; }
      }
      // Drain only newly appended bytes. The retained descriptor owns the read
      // cursor; a later notification never reparses previously consumed rows.
      while (!settled) {
        const { bytesRead } = await file.read(buffer);
        if (!bytesRead) return;
        const text = decoder.write(buffer.subarray(0, bytesRead));
        let start = 0, end;
        while ((end = text.indexOf("\n", start)) !== -1) {
          fragments.push(text.slice(start, end));
          const line = fragments.join("");
          fragments = [];
          start = end + 1;
          if (!line) continue;
          const event = JSON.parse(line);
          if (match(event)) { settled = true; found(event); return; }
        }
        if (start < text.length) fragments.push(text.slice(start));
      }
    }).catch(failed);
  };
  watcher.on("change", scan);
  watcher.on("error", failed);
  scan();
  try {
    return await Promise.race([ready, closed.then(result => {
      throw new Error(`hook source exited before its event: ${JSON.stringify(result)}`);
    })]);
  } finally {
    settled = true;
    watcher.close();
    await reads;
    await file?.close();
  }
}
