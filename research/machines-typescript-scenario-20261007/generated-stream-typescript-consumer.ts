// Generated from Rust scenario stream/typescript-consumer.
// Rust output SHA256: sha256:a57089eda97464fb3aebe5075bb90dd9e7a83b38c4df40a51d47b854e0cd29c1
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
if (records.length !== expected.length || records.some((record, index) => record.some((byte, offset) => byte !== expected[index][offset]))) throw new Error("Rust read parity failed");
console.log(JSON.stringify({ tail: tail.toString(), records: records.length }));
