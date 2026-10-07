import { startConformanceFixture } from "file:///tmp/c8-fixture-sdk/typescript/packages/actors/test/grpc-conformance-fixture.mjs";
import { writeFile } from "node:fs/promises";
const output=process.argv[2]; const fixture=await startConformanceFixture(); await writeFile(output,JSON.stringify(fixture.options)); const close=async()=>{try{await fixture.close()}finally{process.exit(0)}}; process.on("SIGINT",close); process.on("SIGTERM",close); setInterval(()=>{},1000);
