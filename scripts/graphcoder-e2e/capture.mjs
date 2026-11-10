// Execute inside the real shell's selected task view on every OS.
import { readFileSync, writeFileSync } from "node:fs";
writeFileSync("bash.txt", "BASH_E2E_CONTENT\n");
// Cross the presentation boundary before the final marker, retaining the
// complete output through both native and plain-directory command capture.
process.stdout.write("x".repeat(32_768));
process.stdout.write(readFileSync("bash.txt"));
