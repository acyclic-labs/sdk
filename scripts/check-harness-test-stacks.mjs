import { readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function checkHarnessTestStacks(root) {
  const harness = join(root, "rust/crates/harness");
  for (const path of readdirSync(harness, { recursive: true })) {
    if (!path.endsWith(".rs")) continue;
    const source = readFileSync(join(harness, path), "utf8");
    if (/\.\s*stack_size\s*\(|set_var\s*\(\s*"RUST_MIN_STACK"/u.test(source)) {
      throw new Error(`Harness stack enlargement: ${path}`);
    }
  }
  const packageScript = readFileSync(join(root, "scripts/check-harness-package.sh"), "utf8");
  if (/\bRUST_MIN_STACK\s*=/u.test(packageScript)) {
    throw new Error("Harness stack enlargement: scripts/check-harness-package.sh");
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  checkHarnessTestStacks(process.cwd());
}
