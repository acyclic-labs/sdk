import { ActorsClient } from "@acyclic-labs/actors";
const client = new ActorsClient({ endpoint: "http://127.0.0.1:1", token: "fixture-token" });
try { console.log("transport=" + await client.transport); } catch (error) { console.log("error=" + error.message); }
