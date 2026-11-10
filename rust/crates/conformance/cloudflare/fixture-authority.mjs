import init, { HostFixtureAuthority, hostFixtureGrants, hostFixtureTrust } from "./generated/acyclic_cloudflare_conformance.js";

/** Load the qualification host into memory separate from the harness guest. */
export async function initializeFixtureAuthority() {
  const source = new URL("./generated/acyclic_cloudflare_conformance_bg.wasm", import.meta.url);
  await init({ module_or_path: globalThis.Bun ? await Bun.file(source).arrayBuffer() : source });
}

/** An isolated host owns all agents in its task under an explicit grant ceiling.
 * Only public evidence and typed callbacks cross into the harness module. */
export function fixtureAuthority(task, grants = hostFixtureGrants(), account = "browser") {
  const closed = new AbortController();
  const now = () => BigInt(Math.floor(performance.timeOrigin + performance.now()));
  const deadline = (now() / 1000n + 3599n) * 1000n;
  // The host owns one monotonic wall-time projection for the original lifetime
  // and every narrowed operation. Guests retain this port, never a new clock.
  const clock = {
    nowUnixMilliseconds() {
      if (closed.signal.aborted || now() >= deadline) throw new Error("fixture authority closed");
      return now();
    },
    expiresAt(expiry, signal) {
      const cancelled = AbortSignal.any([closed.signal, signal]);
      if (cancelled.aborted) return Promise.resolve("cancelled");
      const remaining = (expiry < deadline ? expiry : deadline) - now();
      if (remaining <= 0n) return Promise.resolve("deadline");
      return new Promise(resolve => {
        const finish = reason => {
          clearTimeout(timer);
          cancelled.removeEventListener("abort", abort);
          resolve(reason);
        };
        const abort = () => finish("cancelled");
        const timer = setTimeout(() => finish("deadline"), Number(remaining));
        cancelled.addEventListener("abort", abort, { once: true });
      });
    },
  };
  const stopped = clock.expiresAt(deadline, closed.signal);
  const host = new HostFixtureAuthority(task, grants, account, stopped, deadline, clock);
  const admission = host.admission();
  admission.caller = Uint8Array.from(admission.caller);
  const capability = {
    admission, stopped, clock,
    trustChanged: signal => host.trustChanged(signal),
    trustClosed: () => host.trustClosed(),
    check: scope => closed.signal.aborted || now() >= deadline ? "closed" : host.check(scope),
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
  let access;
  const projections = Object.freeze({
    platformAuthority: () => capability,
    hostAccess: () => {
      access ??= Object.freeze({
        trust: Object.freeze({ trust: new TextEncoder().encode(hostFixtureTrust()), audience: "workers:/test", holder: "account" }),
        credential: host.bindingCredential(),
        policy(_principal, _command, signal) {
          signal.throwIfAborted();
          return capability.check(admission.scope) === "current";
        },
      });
      return access;
    },
  });
  // The descriptor selects a port; only this original host can produce it.
  const bindings = Object.freeze({ binding(kind) {
    if (closed.signal.aborted || now() >= deadline)
      throw new Error("guest admission stopped; recovery: obtain a fresh authenticated invocation before guest entry");
    return host.binding(kind, projections[kind]);
  } });
  return { capability, bindings, revoke: scope => host.revoke(scope), close: () => closed.abort(),
    free() { closed.abort(); access?.credential.fill(0); host.free(); } };
}
