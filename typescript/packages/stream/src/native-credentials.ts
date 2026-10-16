import type { CredentialProvider } from "@acyclic-labs/actors/credential";

/** Physical RPCs share authentication; cancelling a cursor releases only its interest. */
export function nativeCredentials(credential: CredentialProvider, abandon: () => void = () => {}) {
  const active = new Set<object>();
  const pending = new Set<AbortController>();
  return {
    async resolve(): Promise<string> {
      if (active.size === 0) throw new DOMException("Stream request cancelled", "AbortError");
      const controller = new AbortController();
      pending.add(controller);
      try { return await credential(controller.signal); }
      finally { pending.delete(controller); }
    },
    retain(signal?: AbortSignal): () => void {
      signal?.throwIfAborted();
      const interest = {};
      active.add(interest);
      const release = () => {
        signal?.removeEventListener("abort", release);
        if (!active.delete(interest)) return;
        if (active.size === 0) {
          // Fence the Rust dial before abort can synchronously start a new cursor.
          abandon();
          // Snapshot before invoking abort callbacks, which may open a new cursor.
          for (const controller of [...pending]) controller.abort();
        }
      };
      signal?.addEventListener("abort", release, { once: true });
      return release;
    },
  };
}
