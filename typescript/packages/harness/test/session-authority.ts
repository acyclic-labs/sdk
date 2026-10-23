/** Ephemeral test runtime: roots stay here; clients receive one workload tip. */
import { create, toBinary } from "@bufbuild/protobuf";
import { BirthSchema, BlockSchema, CapabilitySchema, CertifiedKeySchema, PrincipalKind } from "../../auth/generated/proto/auth/v1/auth_pb.js";
import { WasmSessionAuthority, type WasmStream } from "../src/client-query.js";

const encoder = new TextEncoder();
function joined(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((size, part) => size + part.length, 0));
  let offset = 0;
  for (const part of parts) { result.set(part, offset); offset += part.length; }
  return result;
}
function integer(value: bigint, bytes: number): Uint8Array {
  const result = new Uint8Array(bytes);
  for (let index = 0; index < bytes; index++) { result[index] = Number(value & 255n); value >>= 8n; }
  return result;
}
async function key() {
  const pair = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]) as CryptoKeyPair;
  return { ...pair, publicBytes: new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)) };
}
async function sign(key: CryptoKey, transcript: Uint8Array) {
  return new Uint8Array(await crypto.subtle.sign("Ed25519", key, transcript));
}
export async function bindLocalSession(stream: WasmStream): Promise<void> {
  const environment = await key(), account = await key(), runtime = await key(), tip = await key();
  const now = BigInt(Math.floor(Date.now() / 1000));
  const birth = create(BirthSchema, { accountId: "account", environment: "fixture", genesisPublicKey: account.publicBytes, issuedAtUnixSeconds: now - 60n });
  const certified = create(CertifiedKeySchema, {
    accountId: "account", keyId: "runtime", publicKey: runtime.publicBytes, keyEpoch: 1n,
    validity: { notBeforeUnixSeconds: now - 60n, expiresAtUnixSeconds: now + 60n },
    grants: { grants: [{ kind: { case: "resource", value: { resource: { name: "streams:/", prefix: true }, actions: ["streams/follow", "streams/list"] } } }] },
    principal: { kind: PrincipalKind.SERVICE, id: "runtime" },
  });
  const block = create(BlockSchema, {
    certificates: {
      birth: { birth, signature: await sign(environment.privateKey, joined(encoder.encode("acyclic.auth.v1/birth\0"), toBinary(BirthSchema, birth))) },
      keys: [{ key: certified, signature: await sign(account.privateKey, joined(encoder.encode("acyclic.auth.v1/key\0"), toBinary(CertifiedKeySchema, certified))) }],
    },
    caveats: [{ kind: { case: "audience", value: { name: "workers:/test", prefix: false } } }],
    nextPublicKey: tip.publicBytes,
  });
  const payload = toBinary(BlockSchema, block);
  const signature = await sign(runtime.privateKey, joined(encoder.encode("acyclic.auth.v1/block\0"), integer(0n, 8), integer(1n, 4), new Uint8Array(32), integer(BigInt(payload.length), 8), payload));
  const privateTip = new Uint8Array(await crypto.subtle.exportKey("pkcs8", tip.privateKey));
  const credential = toBinary(CapabilitySchema, create(CapabilitySchema, { blocks: [{ payload, signature }], proof: { case: "secret", value: privateTip.slice(-32) }, version: 1 }));
  privateTip.fill(0);
  const authority = new WasmSessionAuthority(JSON.stringify({ environment: "fixture", environment_keys: [Array.from(environment.publicBytes)], account: "account", issuer_keys: [Array.from(account.publicBytes)], minimum_key_epoch: 1, revoked_key_ids: [], revoked_block_ids: [] }), "workers:/test", "account");
  try { stream.bindSession(authority); stream.registerCredential(0, credential); }
  finally { credential.fill(0); authority.free(); }
}
