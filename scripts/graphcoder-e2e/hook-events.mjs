import { watch } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname } from "node:path";

// Subscribe before taking the snapshot. Every later read is driven by a file
// notification; process exit cancels the subscription without a readiness loop.
export async function waitForHook(path, match, closed) {
  let found, failed;
  const ready = new Promise((resolve, reject) => { found = resolve; failed = reject; });
  const watcher = watch(dirname(path));
  let reads = Promise.resolve();
  const scan = () => {
    reads = reads.then(async () => {
      let text;
      try { text = await readFile(path, "utf8"); }
      catch (error) { if (error.code === "ENOENT") return; throw error; }
      // A writer may still own an incomplete final line. Only complete records
      // are observations; the next filesystem event covers its later append.
      for (const line of text.slice(0, text.lastIndexOf("\n") + 1).split("\n").filter(Boolean)) {
        const event = JSON.parse(line);
        if (match(event)) { found(event); return; }
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
    watcher.close();
    await reads;
  }
}
