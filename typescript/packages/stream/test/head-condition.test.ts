import { expect, test } from "bun:test";
import { StreamClient, commitId, idempotencyKey } from "../src/index.js";
import { encodeHttpRequest, normalizeWireCommit, normalizeWireCommitBytes } from "../src/contract.js";

const key = (value: string) => idempotencyKey(new TextEncoder().encode(value));

test("head conditions retain commit identity through the client, WASM provider and retries", async () => {
  const client = StreamClient.memory();
  const stream = client.bytes("head");
  const first = await stream.append(new Uint8Array([1]));
  if (!first.ok) throw new Error("seed failed");
  const wrong = commitId(new Uint8Array(32).fill(42));
  const result = await client.commit({
    conditions: [{ stream, ifTail: 1n, ifCommitId: wrong }],
    mutations: [{ append: { stream, values: [new Uint8Array([2])] } }],
  }, { idempotencyKey: key("wrong") });
  expect(result).toEqual({ ok: false, code: "conflict", conflicts: [{ path: "head", expectedCommitId: wrong, actualCommitId: first.commitId }] });
  expect(await stream.tail()).toBe(1n);
  const request = {
    conditions: [{ stream, ifTail: 1n, ifCommitId: first.commitId }],
    mutations: [{ append: { stream, values: [new Uint8Array([2])] } }],
  };
  const accepted = await client.commit(request, { idempotencyKey: key("correct") });
  expect(accepted.ok).toBe(true);
  expect(await client.commit(request, { idempotencyKey: key("correct") })).toEqual(accepted);
  expect(await stream.tail()).toBe(2n);
});

test("head normalization keeps the hash fence and rejects an empty-history anchor", async () => {
  const identity = commitId(new Uint8Array(32).fill(7));
  const request = { conditions: [{ path: "head", ifTail: 1n, ifCommitId: identity }], mutations: [{ append: { path: "head", values: [new Uint8Array([1])] } }] };
  expect(await normalizeWireCommit(request, { idempotencyKey: key("normalize") })).toEqual(request);
  await expect(normalizeWireCommit({ ...request, conditions: [{ path: "head", ifTail: 0n, ifCommitId: identity }] }, { idempotencyKey: key("empty") })).rejects.toMatchObject({ code: "invalid_argument" });
});

test("HTTP rejects head conditions until the hosted capability is negotiated", async () => {
  const wire = await normalizeWireCommitBytes({
    conditions: [{ path: "head", ifTail: 1n, ifCommitId: commitId(new Uint8Array(32).fill(7)) }],
    mutations: [{ append: { path: "head", values: [new Uint8Array([1])] } }],
  }, { idempotencyKey: key("http") });
  await expect(encodeHttpRequest("commit", wire)).rejects.toMatchObject({ code: "unsupported" });
});
