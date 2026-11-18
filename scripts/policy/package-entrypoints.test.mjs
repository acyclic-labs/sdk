import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { test } from "node:test";

for (const name of ["auth", "actors", "inference"]) {
  test(`${name} package entry point loads from its declared tree`, async () => {
    const require = createRequire(resolve(`wasm_${name}_node_modules/consumer.cjs`));
    await import(pathToFileURL(require.resolve(`@acyclic-labs/${name}`)).href);
  });
}
