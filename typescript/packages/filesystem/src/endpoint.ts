import { validateFilesystemEndpoint } from "./remote-web.js";

/** Parse a service URL before the Rust-owned endpoint policy runs. */
export function secureServiceEndpoint(value: string, invalid: (message: string) => Error): URL {
  let endpoint: URL;
  try { endpoint = new URL(value); }
  catch { throw invalid("endpoint is not a valid absolute URL"); }
  return endpoint;
}

/** Parse and validate a service URL through the canonical Rust policy. */
export async function rustOwnedServiceEndpoint(value: string, invalid: (message: string) => Error): Promise<URL> {
  const endpoint = secureServiceEndpoint(value, invalid);
  try {
    await validateFilesystemEndpoint(value);
  } catch {
    throw invalid("endpoint must use HTTPS or loopback HTTP without credentials, query, or fragment");
  }
  return endpoint;
}
