// Public Actor guest facade for the shared verifier. The artifact producer
// supplies HOSTED_ACTOR_BINDINGS from its complete pinned manifest selection.
import initialize, {ActorsDomain, ActorsPackage} from './generated/acyclic_hosted_actors_conformance.js';
import {WasmActorHost, WasmStream, default as initializeHost} from '../../../../typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js';
import {streamLedgerPort} from '../../../../typescript/packages/harness/src/event-ledger.js';

const encoder = new TextEncoder(), decoder = new TextDecoder('utf-8', {fatal:true});
const root = 'actors/state', controller = 'conformance-controller';
const identity = {name:'acyclic.hosted-actors.conformance', version:'1'};
const empty = new Uint8Array();
const hex = bytes => Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
const ownerName = address => 'conformance-owner-' + hex(address);
const required = (value, name) => {
  if (value === undefined || value === null) throw Error(`ActorBindingUnserved: ${name}`);
  return value;
};
function opened(state) { let value; state.with(current => {value = current;}); return value; }
function text(storage, name) { return storage.transaction(transaction => transaction.get(name)); }
async function put(storage, name, value) {
  storage.transaction(transaction => transaction.put(name, value));
  await storage.flush();
}

async function controllerState(env) {
  const storage = env.state, routes = new Map();
  storage.transaction(transaction => {
    for (const [name, value] of transaction.entries()) if (name.startsWith('owner:')) routes.set(name, value);
  });
  const owner = address => env.namespace.getByName(ownerName(address));
  const request = async (address, path, body = undefined) => {
    const response = await owner(address).fetch('https://conformance.invalid' + path, {method:'POST', body});
    if (!response.ok) throw Error(`HostedActorControlFailed: ${path} ${response.status}`);
    return response;
  };
  let domain;
  let discovering = Promise.resolve();
  const discover = () => discovering = discovering.then(async () => {
    let full;
    do {
      const retained = text(storage, 'cursor');
      const page = await domain.created(retained === undefined ? 0n : BigInt(decoder.decode(retained)));
      for (const address of page.actors) {
        await request(address, '/admit', address);
        const name = 'owner:' + hex(address);
        await put(storage, name, address); routes.set(name, address);
      }
      await put(storage, 'cursor', encoder.encode(String(page.next)));
      full = page.full;
    } while (full);
  });
  domain = await ActorsDomain.open(streamLedgerPort(env.streams, controller), async (address, paused) => {
    await discover();
    await request(address, paused ? '/pause' : '/resume');
  }, async () => {
    await discover();
    for (const address of routes.values()) {
      const before = await (await request(address, '/incarnation')).text();
      let stopped = false;
      try { const response = await owner(address).fetch('https://conformance.invalid/reset', {method:'POST'}); stopped = !response.ok; }
      catch { stopped = true; }
      const after = await (await request(address, '/incarnation')).text();
      if (!stopped || before === after) throw Error('HostedActorsPhysicalRestartRequired');
    }
  }, async address => {
    await discover();
    await request(address, '/wake');
  });
  return {domain, storage};
}

function ownerState(env, name) {
  const instance = crypto.randomUUID(), package_ = new ActorsPackage();
  let host, waking = Promise.resolve();
  const current = () => {
    if (!host) {
      const address = required(text(env.state, 'identity'), 'owner identity');
      // Follow cursors use the original streaming binding. ActorStub.fetch is
      // unary and cannot carry a live follow response between these owners.
      const stream = WasmStream.connectProvider(streamLedgerPort(env.streams, name));
      try {
        host = WasmActorHost.open(stream, root, address, package_.manifest(),
          (delivery, signal, interrupt) => package_.onRecord(delivery, signal, interrupt), 1, 64);
      } finally { stream.free(); }
    }
    return host;
  };
  const wake = () => {
    waking = waking.then(async () => {
      const opened = current();
      // Register before the shared host's final committed-input recheck. A
      // changed subscription set must be registered before another wake.
      for (;;) {
        const paths = await opened.wakePaths();
        const placement = await env.streams.register(name, paths);
        if (placement?.admitted !== true) throw Error('HostedActorsWakePlacementUnserved');
        const next = await opened.wake();
        if (JSON.stringify(paths) !== JSON.stringify(await opened.wakePaths())) continue;
        env.state.transaction(transaction => transaction.alarm(next ?? null));
        await env.state.flush();
        return;
      }
    });
    return waking;
  };
  return {wake, async fetch(path, request) {
    if (path === '/admit') {
      const address = new Uint8Array(await request.arrayBuffer());
      const original = text(env.state, 'identity');
      if (original !== undefined && hex(original) !== hex(address)) throw Error('HostedActorIdentityChanged');
      if (ownerName(address) !== name) throw Error('HostedActorAddressMismatch');
      await put(env.state, 'identity', address);
    } else if (path === '/incarnation') return new Response(instance);
    else if (path === '/reset') throw Error('HostedActorsIntentionalPhysicalRestart');
    else if (path === '/wake') await wake();
    else if (path === '/pause' || path === '/resume') {
      await current().pause(path === '/pause');
      if (path === '/resume') await wake();
    } else return new Response(null, {status:404});
    return new Response(null, {status:204});
  }};
}

export const actor = {
  abiVersion:1,
  codec:{identity, encode:() => empty, decode:() => {throw Error('HostedActorRestoresFromNativeState');}},
  async restore(_checkpoint, context) {
    await initialize(); await initializeHost();
    for (const name of HOSTED_ACTOR_BINDINGS.names) required(context.env[name], name);
    const env = {
      state:required(context.env[HOSTED_ACTOR_BINDINGS.state], 'stateTransaction'),
      streams:required(context.env[HOSTED_ACTOR_BINDINGS.streams], 'streams'),
      namespace:required(context.env[HOSTED_ACTOR_BINDINGS.namespace], 'namespace'),
    };
    const name = required(context.actor.id, 'Actor identity');
    return name === controller ? controllerState(env) : ownerState(env, name);
  },
  async event(state, event) {
    if (!['stream', 'timer', 'alarm'].includes(event.type)) throw Error('UnexpectedHostedActorEvent');
    await required(opened(state).wake, 'owner wake')();
    return null;
  },
  checkpoint:() => ({codec:identity, bytes:empty}),
  async fetch(state, request) {
    const value = opened(state), path = new URL(request.url).pathname;
    if (!value.domain) return value.fetch(path, request);
    if (path === '/run') {
      if (text(value.storage, 'ran') !== undefined) throw Error('FreshHostedActorsDomainRequired');
      await put(value.storage, 'ran', Uint8Array.of(1));
      await value.domain.verify();
      return Response.json({suite:'acyclic_actors::conformance::verify', result:'PASS', bindings:HOSTED_ACTOR_BINDINGS.names});
    }
    return new Response(null, {status:404});
  },
};
