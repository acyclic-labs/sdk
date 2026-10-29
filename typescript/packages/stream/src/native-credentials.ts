import { resolveCredential, type CredentialProvider } from "@acyclic-labs/auth/credential";

/** Physical RPCs share authentication; cancelling a cursor releases only its interest. */
export function nativeCredentials(credential: CredentialProvider, abandon: () => void = () => {}) {
  let current: { controller: AbortController; interests: number } | undefined;
  return {
    async resolve(): Promise<string> {
      const cohort = current;
      if (!cohort) throw new DOMException("Stream request cancelled", "AbortError");
      const request = await resolveCredential(credential, cohort.controller.signal);
      cohort.controller.signal.throwIfAborted();
      return request.token();
    },
    retain(signal?: AbortSignal): () => void {
      signal?.throwIfAborted();
      const cohort = current ??= { controller: new AbortController(), interests: 0 };
      cohort.interests++;
      let retained = true;
      const release = () => {
        signal?.removeEventListener("abort", release);
        if (!retained) return;
        retained = false;
        if (--cohort.interests === 0) {
          current = undefined;
          // Fence the Rust dial before abort can synchronously start a new cursor.
          try { abandon(); }
          finally { cohort.controller.abort(); }
        }
      };
      signal?.addEventListener("abort", release, { once: true });
      return release;
    },
  };
}
