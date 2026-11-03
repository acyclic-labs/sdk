import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";

test("decoder fuzz oracles reject planted production defects", async () => {
  const mutations = [
    ["process-report.mjs", "this.#controlFailure ??= cause;", "void cause;", "report"],
    ["process-report.mjs", "    const request = this.#request;",
      "    if (this.#terminal) return;\n    const request = this.#request;", "report"],
    ["process-report.mjs", "lines.end({ requireNewline: true });", "lines.end();", "partial"],
    ["lines.mjs", "this.#decoder.write(chunk)", "Buffer.from(chunk).toString('utf8')", "utf8"],
    ["process-report.fuzz.mjs", "Object.assign(report, selection);",
      'Object.assign(report, selection, JSON.parse(bytes.toString("utf8").split("\\n", 1)[0]));', "foreign"],
    ["assertions.mjs", "if (Array.isArray(value.settled)) for (const child of value.settled)",
      "if (Array.isArray(value.settled)) for (const child of value.settled.slice(0, 1))", "assertions"],
    ["assertions.mjs", 'assert.equal(value.id, id, "reply names a different prompt");',
      "void id;", "assertions"],
    ["assertions.mjs", 'assert.ok(Number.isSafeInteger(value.inbox) && value.inbox >= 0, "missing committed inbox identity");',
      "void value.inbox;", "assertions"],
    ["assertions.mjs", 'assert.equal(typeof value.text, "string", "missing committed answer text");',
      "void value.text;", "assertions"],
    ["assertions.mjs", 'assert.ok(variants[0] === "authored" ? file.generation !== undefined : typeof file.version === "string",',
      "assert.ok(true,", "assertions"],
    ["assertions.mjs", 'value.success === false || (typeof value.exit_code === "number" && value.exit_code !== 0)',
      "value.success === false", "assertions"],
    ["process-report.mjs", 'if (this.#ownerExited && this.#ended) this.#finish("exited");',
      'if (false) this.#finish("exited");', "report"],
    ["process-report.mjs", 'this.#invalid(error.code ?? "transport-error");',
      'this.#finish("write-failed", error.code ?? "transport-error");', "report"],
    ["process-report.mjs", 'Date.now() > request.deadline_unix_ms', "false", "report"],
    ["process-report.mjs", 'this.#finish(this.#ownerExited ? "exited" : "write-failed", error.code ?? "transport-error");',
      'this.#finish(this.#ownerExited ? "exited" : "write-failed", "lost-source-cause");', "report"],
    ["process.mjs", 'if (this.#unique) { this.#settle(new SpoolEnded("duplicate",',
      'if (false) { this.#settle(new SpoolEnded("duplicate",', "spool"],
    ["process.mjs", "if (!this.#state.open) return true;", "if (false) return true;", "spool"],
    ["process.mjs", "if (!this.#path) return Promise.resolve(indexed);",
      "if (!this.#path) return Promise.resolve({});", "spool"],
    ["process.mjs", "this.#last.set(needle, base + at);", "this.#last.set(needle, 0);", "watch"],
    ["process.mjs", "const combined = this.#tail + text, base = this.#offset - this.#tail.length;",
      "const combined = text, base = this.#offset;", "watch"],
  ];
  const check = async ([file, before, after, oracle]) => {
    // Mutate the actual imported implementation, never a parallel model. A
    // missing mutation site, import error or crash cannot qualify the oracle.
    const script = `
      import { registerHooks } from 'node:module';
      import { readFileSync } from 'node:fs';
      import assert from 'node:assert/strict';
      let applied = false;
      registerHooks({ load(url, context, next) {
        const loaded = next(url, context);
        if (!url.endsWith(${JSON.stringify("/" + file)})) return loaded;
        const source = String(loaded.source);
        assert.equal(source.split(${JSON.stringify(before)}).length, 2, 'mutation site must be unique');
        applied = true;
        return { ...loaded, source: source.replace(${JSON.stringify(before)}, ${JSON.stringify(after)}) };
      } });
      try {
        ${oracle === "watch" ? `
          const { fuzz } = await import('./output-watch.fuzz.mjs');
          fuzz(Buffer.alloc(0));
        ` : oracle === "spool" ? `
          const { fuzz } = await import('./spool.fuzz.mjs');
          await fuzz(Buffer.from([123, 10]));
        ` : oracle === "assertions" ? `
          const { fuzz } = await import('./assertions.fuzz.mjs');
          fuzz(Buffer.alloc(0));
        ` : oracle === "utf8" ? `
          const { fuzz } = await import('./lines.fuzz.mjs');
          const { captures } = JSON.parse(readFileSync('./fixtures/shipping-output.json'));
          fuzz(Buffer.from(captures['text-responses'].stdout));
        ` : `
          const { fuzz } = await import('./process-report.fuzz.mjs');
          ${oracle === "report" ? "await fuzz(Buffer.alloc(0));" : `
            const { ready, acknowledgement } = JSON.parse(readFileSync('./fixtures/native-report-ready.json'));
            const wire = value => Buffer.from(JSON.stringify(value) + '\\n');
            ${oracle === "foreign" ? `
              const result = await fuzz(wire({ ...acknowledgement, generation: '0'.repeat(32) }));
              assert.equal(result.termination.report_acknowledged, false);
            ` : `
              const result = await fuzz(Buffer.concat([wire(acknowledgement), wire(ready).subarray(0, -2)]));
              assert.equal(result.termination.control_failure, 'unterminated-control-frame');
            `}
          `}
        `}
      } catch (error) {
        if (!applied || error.code !== 'ERR_ASSERTION') throw error;
        console.log('PLANTED_DEFECT_REJECTED');
        process.exitCode = 1;
      }
      assert.ok(applied, 'production mutation must execute');
    `;
    const result = await new Promise(resolve => execFile(process.execPath, ["--input-type=module", "--eval", script], {
      cwd: new URL(".", import.meta.url), encoding: "utf8", windowsHide: true,
    }, (error, stdout, stderr) => resolve({ error, stdout, stderr })));
    const recovery = `step=detect planted ${file} defect; recovery=node --test scripts/graphcoder-e2e/decoder-fuzz-negative.test.mjs`;
    assert.equal(result.error?.signal, null, recovery);
    assert.equal(result.error?.code, 1, `${recovery}\n${result.stdout}\n${result.stderr}`);
    assert.match(result.stdout, /PLANTED_DEFECT_REJECTED/, `${recovery}\n${result.stdout}\n${result.stderr}`);
  };
  const remaining = mutations.values();
  // Two isolated children share admission credit; each close releases one slot.
  await Promise.all([0, 1].map(async () => {
    for (const mutation of remaining) await check(mutation);
  }));
});
