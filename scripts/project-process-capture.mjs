import { copyFileSync, mkdirSync } from "node:fs";

// Arena ships the same capture primitive used by repository tooling. Its
// standalone package includes the projection emitted by TypeScript, so runtime
// commands never depend on a repository checkout or a second implementation.
const output = new URL("../arena/packages/arena/src/generated/", import.meta.url);
mkdirSync(output, { recursive: true });
copyFileSync(new URL("./process-capture.mjs", import.meta.url), new URL("process-capture.mjs", output));
