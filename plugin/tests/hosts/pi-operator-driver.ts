import assert from "node:assert/strict";
import { lstatSync, readFileSync, realpathSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";

const preparedType = "acyclic.writeback.prepared";

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

interface PiOperatorDriverOptions {
  session: string;
  cwd: string;
  command: (args: string[]) => Promise<{ stdout: string }>;
  selected: Readonly<Record<string, { baseline: string | null; candidate: string | null; current: string | null }>>;
  preserved: Readonly<Record<string, string>>;
  operation: string;
}

/** Outside-model driver: only the real protected-root CLI may decide a ticket. */
export function piOperatorDriver(options: PiOperatorDriverOptions) {
  let decision: Promise<Record<string, unknown>> | undefined;
  const operation = options.operation;
  assert.ok(isAbsolute(options.cwd));
  assert.ok(options.session);
  assert.ok(Object.keys(options.selected).length > 0);
  for (const [path, oracle] of Object.entries(options.selected)) {
    assert.equal(physical(path), oracle.baseline, `independent initial Root baseline differs: ${path}`);
  }

  async function command(args: string[]): Promise<Record<string, unknown>> {
    const {stdout} = await options.command(args);
    const result: unknown = JSON.parse(stdout);
    assert.ok(isJsonRecord(result));
    return result;
  }

  async function inspect(projectFiles = false): Promise<Record<string, unknown>> {
    return command(["pi", "approvals", "--session", options.session, "--operation", operation, "--json",
      ...(projectFiles ? ["--project-files"] : [])]);
  }

  async function approve(digest: string): Promise<Record<string, unknown>> {
    return command(["pi", "approve", "--session", options.session, "--operation", operation,
      "--digest", digest, "--decision", "approve"]);
  }

  function consumeJsonLine(line: string): void {
    const event: unknown = JSON.parse(line);
    if (!isJsonRecord(event)) return;
    if (event.type !== "tool_execution_update") return;
    const update = isJsonRecord(event.partialResult) ? event.partialResult : undefined;
    const descriptor = update?.details;
    if (!isJsonRecord(descriptor) || descriptor.customType !== preparedType) return;
    assert.ok(typeof descriptor.operation === "string");
    assert.equal(event.toolName, "acyclic_writeback");
    assert.equal(event.toolCallId, descriptor.operation, "prepared event changed its original SDK tool identity");
    assert.equal(descriptor.operation, operation);
    if (decision) return;
    decision = auditAndApprove();
    void decision.catch(() => {});
  }

  function physical(path: string): string | null {
    const absolute = resolve(options.cwd, path);
    const selected = relative(options.cwd, absolute);
    assert.ok(selected && selected !== ".." && !selected.startsWith(`..${sep}`) && !isAbsolute(selected));
    try {
      const metadata = lstatSync(absolute);
      assert.ok(metadata.isFile() && !metadata.isSymbolicLink(), "operator oracle requires a real selected regular file");
      assert.ok(metadata.size <= 1024 * 1024);
      return readFileSync(absolute, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
  }

  async function auditAndApprove(): Promise<Record<string, unknown>> {
    const inspection = await inspect(true);
    assert.equal(inspection.operation, operation);
    assert.equal(inspection.status, "prepared");
    assert.equal(typeof inspection.approval_digest, "string");
    assert.deepEqual(inspection.paths, Object.keys(options.selected));
    const prepared = inspection.prepared;
    assert.ok(isJsonRecord(prepared) && typeof prepared.root_path === "string");
    assert.equal(realpathSync(prepared.root_path), realpathSync(options.cwd));
    assert.equal(prepared.session, options.session);
    assert.equal(inspection.file_evidence_complete, true);
    assert.equal(inspection.file_evidence_cursor, null);
    const importedFiles = inspection.imported_selected_files;
    assert.ok(isJsonRecord(importedFiles));
    assert.deepEqual(Object.keys(importedFiles).sort(), Object.keys(options.selected).sort());
    for (const [path, oracle] of Object.entries(options.selected)) {
      assert.equal(physical(path), oracle.current);
      const encoded = importedFiles[path];
      if (oracle.candidate === null) assert.equal(encoded, null);
      else {
        assert.ok(typeof encoded === "string" && encoded.length <= 2 * 1024 * 1024);
        const bytes = Buffer.from(encoded, "base64");
        assert.equal(bytes.toString("base64"), encoded);
        assert.deepEqual(bytes, Buffer.from(oracle.candidate, "utf8"));
      }
    }
    for (const [path, contents] of Object.entries(options.preserved)) assert.equal(physical(path), contents);
    await wrongDigest();
    return approve(String(inspection.approval_digest));
  }

  async function assertPublished(): Promise<void> {
    assert.ok(decision);
    await decision;
    const inspection = await inspect();
    assert.equal(inspection.status, "published");
    assert.equal(inspection.operation, operation);
    assert.ok(isJsonRecord(inspection.receipt));
    assert.equal(inspection.receipt.phase, "Applied", "operator decision is not a native publication receipt");
    for (const [path, oracle] of Object.entries(options.selected)) assert.equal(physical(path), oracle.candidate);
    for (const [path, contents] of Object.entries(options.preserved)) assert.equal(physical(path), contents);
  }

  function assertRootUnpublished(): void {
    for (const [path, oracle] of Object.entries(options.selected)) assert.equal(physical(path), oracle.current);
    for (const [path, contents] of Object.entries(options.preserved)) assert.equal(physical(path), contents);
  }

  async function wrongDigest(): Promise<void> {
    const before = await inspect();
    assertRootUnpublished();
    const digest = String(before.approval_digest);
    const wrong = `${digest[0] === "0" ? "1" : "0"}${digest.slice(1)}`;
    await assert.rejects(approve(wrong));
    const after = await inspect();
    assert.equal(after.status, "prepared");
    assert.equal(after.approval_digest, digest);
    assert.deepEqual(after.prepared, before.prepared, "rejected digest changed the original prepared ticket");
    assertRootUnpublished();
  }

  return { consumeJsonLine, assertPublished };
}
