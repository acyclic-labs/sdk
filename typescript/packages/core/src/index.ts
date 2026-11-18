export * from "./observe.js";
export * from "./deadline.js";

/** Validates an optional transport byte limit while preserving an omitted limit. */
export function validateByteLimit(value: number | undefined, option: string): number | undefined {
  if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) {
    throw new RangeError(`${option} must be a positive safe integer`);
  }
  return value;
}

/** Checks the endpoint shape shared by the SDK transports. */
export function isTransportEndpoint(url: URL, allowLoopbackHttp = false): boolean {
  if (url.username || url.password || url.search || url.hash) return false;
  return url.protocol === "https:" || (allowLoopbackHttp && url.protocol === "http:"
    && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname));
}
