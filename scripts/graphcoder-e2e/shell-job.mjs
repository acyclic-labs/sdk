// The shipping shell launches this child. A TCP event, never a timer or a
// readiness probe, controls completion across a committed conversation turn.
import { connect } from "node:net";
import { readFileSync, writeFileSync } from "node:fs";
import { once } from "node:events";
import { verifySpill } from "./spill.mjs";
const [name] = process.argv.slice(2);
if (name === "cancel") {
  writeFileSync(process.argv[3], "cancel\n");
  process.exit(0);
}
if (name === "verify") {
  console.log(verifySpill(process.argv[3], 80_000,
    ["JOB_READY_two", "JOB_STDERR_two", "JOB_DONE_two"]));
  process.exit(0);
}
const { port } = JSON.parse(readFileSync("shell-gate.json", "utf8"));
const gate = connect({ host: "127.0.0.1", port });
await once(gate, "connect");
gate.write(name + "\n");
console.log("JOB_READY_" + name);
const [release] = await once(gate, "data");
if (release.toString() !== "G") throw new Error("unexpected job release");
await new Promise(resolve => process.stdout.write("x".repeat(80_000), resolve));
await new Promise(resolve => process.stderr.write("JOB_STDERR_" + name + "\n", resolve));
await new Promise(resolve => process.stdout.write("JOB_DONE_" + name + "\n", resolve));
writeFileSync(name + ".completed", "committed\n");
gate.end("OUTPUT_COMPLETE\n");
