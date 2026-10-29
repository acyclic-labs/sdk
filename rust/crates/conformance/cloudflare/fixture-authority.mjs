import init, { HostFixtureAuthority, hostFixtureGrants } from "./generated/acyclic_cloudflare_conformance.js";

/** Load the qualification host into memory separate from the harness guest. */
export async function initializeFixtureAuthority() {
  const source = new URL("./generated/acyclic_cloudflare_conformance_bg.wasm", import.meta.url);
  await init({ module_or_path: globalThis.Bun ? await Bun.file(source).arrayBuffer() : source });
}

/** An isolated host owns all agents in its task under an explicit grant ceiling.
 * Only public evidence and typed callbacks cross into the harness module. */
export function fixtureAuthority(task, grants = hostFixtureGrants(), account = "browser") {
  const closed = new AbortController();
  const deadline = BigInt(Math.floor(Date.now() / 1000) + 3599) * 1000n;
  // This is the host invocation's original expiry event, shared by both modules.
  const lifetime = AbortSignal.any([closed.signal, AbortSignal.timeout(Number(deadline) - Date.now())]);
  const stopped = new Promise(resolve => lifetime.addEventListener("abort", () =>
    resolve(closed.signal.aborted ? "cancelled" : "deadline"), { once: true }));
  const host = new HostFixtureAuthority(task, grants, account, stopped, deadline);
  const admission = host.admission();
  admission.caller = Uint8Array.from(admission.caller);
  const capability = {
    admission, stopped,
    trustChanged: signal => host.trustChanged(signal),
    check: scope => lifetime.aborted || BigInt(Date.now()) >= deadline ? "closed" : host.check(scope),
    derive(request, signal) {
      signal.throwIfAborted();
      if (this.check(request.parent) !== "current") return { kind: "closed" };
      return host.derive(request);
    },
    checkpoint(request, signal) {
      signal.throwIfAborted();
      if (this.check(admission.scope) !== "current") return { kind: "closed" };
      return host.checkpoint(request);
    },
  };
  return { capability, revoke: scope => host.revoke(scope), close: () => closed.abort(), free() { closed.abort(); host.free(); } };
}
