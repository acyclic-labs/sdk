export * from "./observe.js";
export * from "./deadline.js";

/** Validates an optional transport byte limit while preserving an omitted limit. */
export function validateByteLimit(value: number | undefined, option: string): number | undefined {
  if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) {
    throw new RangeError(`${option} must be a positive safe integer`);
  }
  return value;
}
