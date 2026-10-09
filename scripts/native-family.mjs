import { readFileSync } from "node:fs";
function immutable(value) {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}
/** Read committed Rust-generated family facts; importing this module never runs Cargo. */
export function nativeFamily(key) {
  const value = JSON.parse(readFileSync(new URL(`./generated/native-families/${key}.json`, import.meta.url), "utf8"));
  if (value.schema !== "acyclic.native-family.v1" || value.key !== key) throw new Error("Rust native family identity differs");
  return immutable(value);
}
