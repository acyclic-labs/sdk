import { ActorsClient } from "@acyclic-labs/actors";
console.log("platform="+process.platform+" glibc="+(process.report.getReport().header.glibcVersionRuntime ?? "none"));
try { console.log("transport="+await new ActorsClient({endpoint:"http://127.0.0.1:1",token:"fixture-token"}).transport); }
catch(e) { console.log("error="+e.message); }
