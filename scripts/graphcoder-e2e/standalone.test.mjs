import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join, relative, resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "./process.mjs";
import { environment } from "./scratch.mjs";

for (const entry of ["qualify-web-lifecycle.mjs", "qualify-fetch-cancellation.mjs"]) {
  test(`${entry} admits subprocesses through the shared scratch and profile owner`, async () => {
    const root = mkdtempSync(join(tmpdir(), "standalone-fixture-contract-"));
    const processes = new Processes();
    try {
      const scratch = join(root, "leased"), inherited = join(root, "inherited");
      mkdirSync(scratch); mkdirSync(inherited);
      const binary = join(root, "shipping-boundary-probe"), capture = join(root, "capture.json");
      const preload = join(root, "admission.mjs");
      // Execute the real public entry point, then inspect its actual spawn boundary.
      // Exit before a shipping process starts; this is fixture proof, not acceptance.
      writeFileSync(preload, `import cp from 'node:child_process';
import {syncBuiltinESMExports} from 'node:module';
import {writeFileSync} from 'node:fs';
const original=cp.spawn;
cp.spawn=(program,args,options)=>{
 if(program===${JSON.stringify(binary)} || args.includes(${JSON.stringify(binary)})){
  const env=options.env;
  writeFileSync(${JSON.stringify(capture)},JSON.stringify({home:env.HOME,temp:env.TEMP,tmp:env.TMP,tmpdir:env.TMPDIR,authPresent:'FIXTURE_AUTH_TOKEN' in env}));
  process.exit(86);
 }
 return original(program,args,options);
};syncBuiltinESMExports();`);
      const result = await processes.run(process.execPath,
        [fileURLToPath(new URL(`../${entry}`, import.meta.url)), binary], {
          env: { ...environment(join(root, "parent")), E2E_SCRATCH_ROOT: scratch,
            E2E_CONPTY_HOST: join(root, "unused-intercepted-conpty-host"),
            TEMP: inherited, TMP: inherited, TMPDIR: inherited,
            FIXTURE_AUTH_TOKEN: "synthetic-contract-marker",
            NODE_OPTIONS: `--import=${pathToFileURL(preload).href}` },
        });
      assert.equal(result.code, 86, result.stderr);
      const actual = JSON.parse(readFileSync(capture, "utf8"));
      const within = relative(scratch, actual.home);
      assert.ok(within && within !== ".." && !within.startsWith("..") && !isAbsolute(within),
        "the public suite must allocate its fixture under the admitted scratch root");
      for (const key of ["temp", "tmp", "tmpdir"])
        assert.equal(resolve(actual[key]), resolve(actual.home, "tmp"), key);
      assert.equal(actual.authPresent, false, "shared profiles must strip inherited auth tokens");
    } finally { await processes.close(); rmSync(root, { recursive: true, force: true }); }
  });
}
