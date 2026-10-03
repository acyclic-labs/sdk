import { runCli } from "./terminal.js";

process.exitCode = await runCli(process.argv.slice(2));
