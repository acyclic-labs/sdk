import { HttpStreamProvider } from "@acyclic-labs/stream";

const streams = new HttpStreamProvider({
  endpoint: process.env.STREAM_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
console.log(await streams.tail(process.env.STREAM_PATH!));
