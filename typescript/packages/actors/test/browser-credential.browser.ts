import { ActorsClient } from "../dist/client.js";
import { CredentialCache, CredentialSnapshot, resolveCredential } from "@acyclic-labs/auth/credential";

function check(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

export async function test(endpoint: string) {
  let refreshes = 0;
  let revision = "account/audience/1";
  const cache = new CredentialCache(async () => new CredentialSnapshot(
    ++refreshes === 1 ? "synthetic-rejected" : "synthetic-current", revision, 100,
  ), () => ({ nowUnixSeconds: 1, revision }));
  const client = new ActorsClient({ endpoint, credential: cache.provider });
  const old = await resolveCredential(cache.provider);
  const changed = cache.provider.watch!();
  check(old.binding?.revision === revision, "admitted source revision was lost");
  const call = async (expected: string, rejected: boolean) => {
    try { await client.inspectActor({ actorId: "credential-control" }); }
    catch (error) {
      const result = error as { code?: string; metadata?: { credentialRejected?: boolean } };
      check(result.code === expected && result.metadata?.credentialRejected === rejected, "default WASM bridge lost the remote verdict");
      return;
    }
    throw new Error("service refusal unexpectedly succeeded");
  };
  await call("unauthenticated", true);
  check(changed.changed.aborted, "exact rejection did not notify the reader");
  const current = await resolveCredential(cache.provider);
  const retained = cache.provider.watch!();
  check(!old.reject(), "late rejection invalidated the replacement");
  await call("permission_denied", false);
  current.check();
  check(!retained.changed.aborted && refreshes === 2, "policy denial invalidated the lease");
  revision = "other-account/audience/2";
  cache.invalidate();
  check(retained.changed.aborted, "source change did not cancel the old reader");
  try { current.check(); } catch { return { requests: 2, refreshes, transport: await client.transport }; }
  throw new Error("old source binding survived the account change");
}
