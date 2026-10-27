/** Exact comparison of Rust client projections, including full-width integers.
 * Escape every data-object key so an integer tag cannot alias user JSON. */
export function clientValueKey(value: unknown): string {
  const encoded = JSON.stringify(value, (_, item: unknown) => {
    if (item instanceof Map) throw new Error("CLIENT_CONFORMANCE client projection contains a Map instead of object fields");
    if (typeof item === "bigint") return {$integer: item.toString()};
    if (typeof item === "number") {
      if (!Number.isFinite(item) || Object.is(item, -0) || (Number.isInteger(item) && !Number.isSafeInteger(item))) {
        throw new Error("CLIENT_INPUT noncanonical or rounded number");
      }
      if (Number.isInteger(item)) return {$integer: item.toString()};
    }
    if (["undefined", "function", "symbol"].includes(typeof item)) {
      throw new Error("CLIENT_INPUT non-JSON client value");
    }
    if (item !== null && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(Object.entries(item)
        .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
        .map(([key, entry]) => [`$${key}`, entry]));
    }
    return item;
  });
  if (encoded === undefined) throw new Error("CLIENT_INPUT absent client value");
  return encoded;
}
