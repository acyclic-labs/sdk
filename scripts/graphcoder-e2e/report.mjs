import { writeFile } from "node:fs/promises";

// The job's retained stdout is the publication boundary. A fixture filesystem
// failure must not make the only copy of its evidence depend on export/GC.
export async function publishReport(path, report, io = {
  publish: text => new Promise((resolve, reject) => process.stdout.write(text,
    error => error ? reject(error) : resolve())),
  writeFile,
}, signal) {
  const cancel = () => {
    if (!signal?.aborted) return false;
    report.pass = false;
    report.cancellation = signal.reason?.message ?? String(signal.reason);
    return true;
  };
  const publish = async () => {
    const text = JSON.stringify(report, null, 2) + "\n";
    await io.publish(text);
    await io.writeFile(path, text);
  };
  const cancelled = cancel();
  await publish();
  // Both original writes have joined. A signal aborts only once, so one
  // terminal failed snapshot closes cancellation during either write.
  if (!cancelled && cancel()) await publish();
}
