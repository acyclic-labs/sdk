import assert from "node:assert/strict";
import test from "node:test";
import { Worker } from "node:worker_threads";

for (const group of ["serial", "decoder", "frames"]) test(`${group} fuzz oracles reject planted production defects`, async () => {
  const mutations = [
    ["serial.mjs", 'assert.deepEqual(first.binding, binding, "partition differs from expected acceptance identity");',
      'void binding;', "serial"],
    ["serial.mjs", 'binding: first.binding', 'binding: undefined', "serial"],
    ["serial.mjs", ",'E2E_GIT_BINARY'", "", "serial-admission"],
    ["serial.mjs", 'if (code === 0) {\n        assert.ok(execution, "missing original suite execution");\n        executionSucceeded(execution);\n      }',
      "void execution;", "serial"],
    ...["expired", "cancelled"].map(field =>
      ["serial.mjs", `!execution?.${field}`, "true", "serial"]),
    ["frames.mjs", "this.#completed = true;", "this.#completed = false;", "frames"],
    ["frames.mjs", "try { this.frame(); } finally { this.#boundary = false; }",
      "this.#boundary = false;", "frames"],
    ["process-report.mjs", "this.#controlFailure ??= cause;", "void cause;", "report"],
    ["process-report.mjs", "    const request = this.#request;",
      "    if (this.#terminal) return;\n    const request = this.#request;", "report"],
    ["process-report.mjs", "lines.end({ requireNewline: true });", "lines.end();", "partial"],
    ["lines.mjs", "this.#decoder.write(chunk)", "Buffer.from(chunk).toString('utf8')", "utf8"],
    ["process-report.mjs", "message.generation !== request.generation",
      "false", "foreign"],
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
    ["process.mjs", "for (const key of this.#key?.(event) ?? []) {",
      "for (const key of (this.#key?.(event) ?? []).slice(0, 1)) {", "spool"],
    ["process.mjs", "indexed = { at, length: bytes.length, written: this.#written };",
      "indexed = { at: 0, length: bytes.length, written: this.#written };", "spool"],
    ["process.mjs", "const bytes = Buffer.alloc(this.#completed), fd = openSync(this.#path, \"r\");",
      "const bytes = Buffer.alloc(0), fd = openSync(this.#path, \"r\");", "spool"],
    ["process.mjs", 'const abort = () => wait.reject(new SpoolEnded("cancelled", "wait cancelled", signal.reason));',
      'const abort = () => wait.reject(new SpoolEnded("cancelled", "wait cancelled", undefined));', "spool"],
    ["process.mjs", 'keyed(key, { signal } = {}) {\n    if (signal?.aborted) return Promise.reject(new SpoolEnded("cancelled", "wait cancelled", signal.reason));',
      'keyed(key, { signal } = {}) {', "spool"],
    ["process.mjs", 'new SpoolEnded("consumer", `step=dispatch event',
      'new SpoolEnded("source", `step=dispatch event', "spool"],
    ["process.mjs", 'stream.once("error", error => this.#settle(new SpoolEnded("source", "source stream failed", error)));',
      'stream.once("error", error => this.#settle(new SpoolEnded("source", "source stream failed", undefined)));', "spool"],
    ["process.mjs", '\n      error => this.#settle(new SpoolEnded("source", "source completion failed", error)));',
      '\n      error => this.#settle(new SpoolEnded("source", "source completion failed", undefined)));', "spool"],
    ["process.mjs", 'if (size === 0) throw new SpoolEnded("spool",',
      'if (size === 0) return new SpoolEnded("spool",', "spool"],
    ["process.mjs", 'catch (error) { throw new SpoolEnded("spool", `transcript ${this.#path} has an invalid indexed event at byte ${at}`, error); }',
      'catch (error) { return null; }', "spool"],
    ["process.mjs", 'if (read === 0) throw new SpoolEnded("spool",',
      'if (read === 0) return new SpoolEnded("spool",', "spool"],
    ["process.mjs", "await this.#written;\n    if (this.#broken) throw this.#broken;",
      "await this.#written;", "spool"],
    ["process.mjs", "if (this.#completed === 0) return [];", "", "spool"],
    ["process.mjs", "if (wait.match?.(event)) {",
      "if ((wait.match?.(event), wait.match?.(event))) {", "spool"],
    ["process.mjs", 'cleanup();\n        if (error) this.#break(error);',
      'cleanup();', "spool"],
    ["process.mjs", 'this.#file.removeListener("drain", blocked);',
      'void blocked;', "spool"],
    ["process.mjs", 'stream.once("close", () => { if (!stream.readableEnded) resolve(); });',
      'stream.once("close", () => {});', "spool"],
    ["process.mjs", 'JSON.stringify(process.argv)}`, error));',
      'JSON.stringify(process.argv)}`, error instanceof Error ? error : undefined));', "spool"],
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
        ${oracle === "serial-admission" ? `
          const { fuzzAdmission } = await import(${JSON.stringify(new URL("./serial.fuzz.mjs", import.meta.url).href)});
          await fuzzAdmission(Buffer.alloc(0));
        ` : oracle === "serial" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./serial.fuzz.mjs", import.meta.url).href)});
          fuzz(Buffer.alloc(0));
        ` : oracle === "frames" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./frames.fuzz.mjs", import.meta.url).href)});
          await fuzz(Buffer.alloc(0));
        ` : oracle === "watch" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./output-watch.fuzz.mjs", import.meta.url).href)});
          fuzz(Buffer.alloc(0));
        ` : oracle === "spool" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./spool.fuzz.mjs", import.meta.url).href)});
          await fuzz(Buffer.from([123, 10]));
        ` : oracle === "assertions" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./assertions.fuzz.mjs", import.meta.url).href)});
          fuzz(Buffer.alloc(0));
        ` : oracle === "utf8" ? `
          const { fuzz } = await import(${JSON.stringify(new URL("./lines.fuzz.mjs", import.meta.url).href)});
          const { captures } = JSON.parse(readFileSync(new URL(${JSON.stringify(new URL("./fixtures/shipping-output.json", import.meta.url).href)})));
          fuzz(Buffer.from(captures['text-responses'].stdout));
        ` : `
          const { fuzz } = await import(${JSON.stringify(new URL("./process-report.fuzz.mjs", import.meta.url).href)});
          ${oracle === "report" ? "await fuzz(Buffer.alloc(0));" : `
            const { ready, acknowledgement } = JSON.parse(readFileSync(new URL(${JSON.stringify(new URL("./fixtures/native-report-ready.json", import.meta.url).href)})));
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
    const result = await new Promise(resolve => {
      const worker = new Worker(new URL("data:text/javascript," + encodeURIComponent(script)), { stdout: true, stderr: true });
      let stdout = "", stderr = "", fault;
      worker.stdout.setEncoding("utf8").on("data", chunk => { stdout += chunk; });
      worker.stderr.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
      worker.once("error", error => { fault = error.stack; });
      worker.once("exit", code => resolve({ error: { code, signal: null }, stdout, stderr, fault }));
    });
    const recovery = `step=detect planted ${file} defect ${JSON.stringify(before)}; recovery=node --test scripts/graphcoder-e2e/decoder-fuzz-negative.test.mjs`;
    assert.equal(result.fault, undefined, `${recovery}\n${result.fault}`);
    assert.equal(result.error?.signal, null, recovery);
    assert.equal(result.error?.code, 1, `${recovery}\n${result.stdout}\n${result.stderr}`);
    assert.match(result.stdout, /PLANTED_DEFECT_REJECTED/, `${recovery}\n${result.stdout}\n${result.stderr}`);
  };
  const category = oracle => oracle.startsWith("serial") ? "serial" : oracle === "frames" ? "frames" : "decoder";
  const remaining = mutations.filter(([, , , oracle]) => category(oracle) === group).values();
  // Each worker has fresh module state and one production mutation. Two share
  // admission credit; the original worker exit releases its slot.
  await Promise.all([0, 1].map(async () => {
    for (const mutation of remaining) await check(mutation);
  }));
});
