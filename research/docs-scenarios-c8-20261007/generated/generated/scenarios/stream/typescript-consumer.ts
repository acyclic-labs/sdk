// Generated from Rust scenario stream/typescript-consumer.
// Rust output SHA256: sha256:81414fe84095e6a30d20f098512ce91a25ac8830a47b4e4bea22abf351436afb
import { StreamClient, idempotencyKey } from "@acyclic-labs/stream";

const path = "typescript/events";
const values = [[123,34,107,105,110,100,34,58,34,99,114,101,97,116,101,100,34,125], [123,34,107,105,110,100,34,58,34,114,101,97,100,121,34,125]].map(value => Uint8Array.from(value));
const retry = idempotencyKey(Uint8Array.from([116,121,112,101,115,99,114,105,112,116,45,115,116,114,101,97,109]));
const stream = StreamClient.memory().bytes(path);
const append = await stream.appendBatch(values, { idempotencyKey: retry });
if (!append.ok || append.tail !== BigInt("2")) throw new Error("Rust append receipt parity failed");
const tail = await stream.tail();
if (tail !== BigInt("2")) throw new Error("Rust tail parity failed");
const records = [];
for await (const record of stream.read({ from: 0n, limit: 2 })) records.push(record.value);
const expected = [[123,34,107,105,110,100,34,58,34,99,114,101,97,116,101,100,34,125], [123,34,107,105,110,100,34,58,34,114,101,97,100,121,34,125]].map(value => Uint8Array.from(value));
const recordsDiffer = records.some((record, index) => {
  const expectedRecord = expected[index];
  return expectedRecord === undefined || record.length !== expectedRecord.length || record.some((byte, offset) => byte !== expectedRecord[offset]);
});
if (records.length !== expected.length || recordsDiffer) throw new Error("Rust read parity failed");
console.log(JSON.stringify({ tail: tail.toString(), records: records.length }));
