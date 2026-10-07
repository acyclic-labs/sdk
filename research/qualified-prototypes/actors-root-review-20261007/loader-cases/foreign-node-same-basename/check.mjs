import { ActorsClient } from "@acyclic-labs/actors";
try { console.log("transport=" + await new ActorsClient({endpoint:"http://127.0.0.1:1",token:"fixture-token"}).transport); } catch (error) { console.log("error="+error.message); }
