/** Resolves the credential for one request. Respect the supplied cancellation signal. */
export type CredentialProvider = (signal?: AbortSignal) => string | Promise<string>;

/** A fixed credential or a provider. A provider takes precedence and never falls back. */
export interface CredentialOptions {
  readonly token?: string;
  readonly credential?: CredentialProvider;
}

/** Validate at each request boundary; never include credential material in errors. */
export function validateCredential(token: unknown): asserts token is string {
  if (typeof token !== "string" || token.length > 12 * 1024 ||
      new TextEncoder().encode(token).byteLength > 12 * 1024 || !token.trim() || /[\r\n\0]/.test(token)) {
    throw new TypeError("credential must be non-empty, at most 12 KiB, and free of CR, LF, or NUL");
  }
}

/** Snapshot configuration without caching provider results or owning renewal state. */
export function credentialProvider(options: CredentialOptions): CredentialProvider {
  const provider = options.credential;
  if (provider !== undefined) {
    if (typeof provider !== "function") throw new TypeError("credential must be a function");
    return signal => new Promise<string>((resolve, reject) => {
      if (signal?.aborted) { reject(signal.reason); return; }
      const cleanup = () => signal?.removeEventListener("abort", abort);
      // Deadlines use the same AbortSignal as transport cancellation.
      const abort = () => { cleanup(); reject(signal?.reason); };
      signal?.addEventListener("abort", abort, { once: true });
      try {
        Promise.resolve(provider(signal)).then(token => {
          cleanup();
          if (signal?.aborted) { reject(signal.reason); return; }
          try { validateCredential(token); resolve(token); }
          catch (error) { reject(error); }
        }, error => { cleanup(); reject(error); });
      } catch (error) { cleanup(); reject(error); }
    });
  }
  const token = options.token;
  validateCredential(token);
  return signal => { signal?.throwIfAborted(); return token; };
}
