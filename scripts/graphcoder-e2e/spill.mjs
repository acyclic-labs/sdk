import { readFileSync } from "node:fs";

// Apply the unchanged byte oracle to the original spill and to a copy with one
// real payload byte removed. Both foreground and background capture use it.
export function verifySpill(path, payloadBytes, markers) {
  const spill = readFileSync(path, "utf8");
  const check = text => {
    if (text.split("x").length - 1 !== payloadBytes ||
      !markers.every(marker => text.includes(marker)))
      throw new Error("complete spill lost output bytes");
  };
  check(spill);
  let rejected = false;
  const offset = spill.indexOf("x");
  try { check(spill.slice(0, offset) + spill.slice(offset + 1)); }
  catch (error) {
    if (error.message !== "complete spill lost output bytes") throw error;
    rejected = true;
  }
  if (!rejected) throw new Error("truncated original spill was accepted");
  return "COMPLETE_SPILL_AND_NEGATIVE_CONTROL";
}
