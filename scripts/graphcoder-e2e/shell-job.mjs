// The shipping shell launches this child. A TCP event, never a timer or a
// readiness probe, controls completion across a committed conversation turn.
import { connect } from "node:net";
import { readFileSync, writeFileSync } from "node:fs";
import { once } from "node:events";
const [name] = process.argv.slice(2);
if (name === "verify") {
  const spill = readFileSync(process.argv[3], "utf8");
  const check = text => {
    if (text.split("x").length - 1 !== 80_000 ||
      !["JOB_READY_two", "JOB_STDERR_two", "JOB_DONE_two"].every(marker => text.includes(marker)))
      throw new Error("complete spill lost output bytes");
  };
  check(spill);
  let rejected = false;
  const offset = spill.indexOf("x");
  try { check(spill.slice(0, offset) + spill.slice(offset + 1)); }
  catch (error) { if (error.message !== "complete spill lost output bytes") throw error; rejected = true; }
  if (!rejected) throw new Error("truncated original spill was accepted");
  console.log("COMPLETE_SPILL_AND_NEGATIVE_CONTROL");
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
