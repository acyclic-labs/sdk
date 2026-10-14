import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { pipeline } from "node:stream/promises";
import { pathToFileURL } from "node:url";

/**
 * @param {string} path
 * @param {string} name
 * @param {number} status
 * @param {import("node:stream").Writable} stdout
 * @param {import("node:stream").Writable} stderr
 */
export async function replayLog(path, name, status, stdout, stderr) {
  /** @param {import("node:stream").Writable} stream @param {string} text */
  const write = (stream, text) => new Promise((resolve, reject) => {
    stream.write(text, error => error ? reject(error) : resolve(undefined));
  });
  // Inherited CI pipes can be nonblocking; await every write, including annotations.
  await write(stdout, `::group::${name}\n`);
  await pipeline(createReadStream(path), stdout, { end: false });
  await write(stdout, "::endgroup::\n");
  if (status !== 0) {
    await write(stdout, `::error::${name} failed with exit status ${status}\n`);
    const lines = [];
    for await (const line of createInterface({ input: createReadStream(path), crlfDelay: Infinity })) {
      lines.push(line);
      if (lines.length > 80) lines.shift();
    }
    if (lines.length) await write(stderr, lines.join("\n") + "\n");
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [path, name, status] = process.argv.slice(2);
  const exitStatus = Number(status);
  if (!path || !name || status === undefined || !Number.isInteger(exitStatus) || exitStatus < 0 || exitStatus > 255)
    throw new Error("usage: ci-background-log.mjs PATH NAME EXIT_STATUS");
  await replayLog(path, name, exitStatus, process.stdout, process.stderr);
}
