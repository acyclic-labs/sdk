import { HttpObjectsV1, verified } from "@acyclic-labs/objects";

const objects = new HttpObjectsV1({ endpoint: process.env.OBJECTS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! });
const put = verified(await objects.put(new TextEncoder().encode("Hello")));
if (!put) throw new Error("outcome unknown; repeating the put is safe");
console.log((await objects.get({ digest: put.digest, range: { start: 1n }, maximumBytes: 4n })).body);
