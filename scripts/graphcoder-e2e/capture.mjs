// Execute inside the real native shell's mounted task view on every OS.
import { readFileSync, writeFileSync } from "node:fs";
writeFileSync("bash.txt", "BASH_E2E_CONTENT\n");
process.stdout.write(readFileSync("bash.txt"));
