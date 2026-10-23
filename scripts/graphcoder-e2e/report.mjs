import { writeFile } from "node:fs/promises";

// The job's retained stdout is the publication boundary. A fixture filesystem
// failure must not make the only copy of its evidence depend on export/GC.
export async function publishReport(path, report, io = {
  publish: text => new Promise((resolve, reject) => process.stdout.write(text,
    error => error ? reject(error) : resolve())),
  writeFile,
}) {
  const text = JSON.stringify(report, null, 2) + "\n";
  await io.publish(text);
  await io.writeFile(path, text);
}
