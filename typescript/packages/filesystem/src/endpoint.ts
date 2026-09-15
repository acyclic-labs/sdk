/** Accepts encrypted service URLs and explicit loopback-only development URLs. */
export function secureServiceEndpoint(value: string, invalid: (message: string) => Error): URL {
  let endpoint: URL;
  try { endpoint = new URL(value); }
  catch { throw invalid("endpoint is not a valid absolute URL"); }
  const loopback = endpoint.hostname === "localhost" || endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]" || endpoint.hostname === "::1";
  if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && loopback)) {
    throw invalid("endpoint must use HTTPS or loopback HTTP");
  }
  if (!endpoint.hostname || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw invalid("endpoint must not contain credentials, query, or fragment");
  }
  return endpoint;
}
