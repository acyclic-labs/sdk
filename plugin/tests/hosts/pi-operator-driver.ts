import assert from "node:assert/strict";
import { lstatSync, readFileSync, realpathSync } from "node:fs";
import { isAbsolute, relative, resolve } from "node:path";

const preparedType = "acyclic.writeback.prepared";

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}


interface OperatorFileOracle {
  baseline: string | null;
  candidate: string | null;
  current: string | null;
}
interface PiOperatorDriverOptions {
  session: string;
  cwd: string;
  command: (args: string[], input?: string) => Promise<{ stdout: string; stderr: string }>;
  selected: Readonly<Record<string, OperatorFileOracle>>;
  preserved: Readonly<Record<string, string>>;
  operation: string;
}

/** Outside-model driver: only the real protected-root CLI may decide a ticket. */
export class PiOperatorDriver {
  private readonly options: PiOperatorDriverOptions;
  private readonly decisions = new Map<string, Promise<Record<string, unknown>>>();

  constructor(options: PiOperatorDriverOptions) {
    assert.ok(isAbsolute(options.cwd));
    assert.ok(options.session);
    assert.ok(Object.keys(options.selected).length > 0);
    this.options = options;
    for (const [path, oracle] of Object.entries(options.selected)) {
      assert.equal(this.physical(options.cwd, path), oracle.baseline, `independent initial Root baseline differs: ${path}`);
    }
  }

  private async command(args: string[], input?: string): Promise<Record<string, unknown>> {
    const {stdout} = await this.options.command(args, input);
    const result: unknown = JSON.parse(stdout);
    assert.ok(result && typeof result === "object" && !Array.isArray(result));
    return result as Record<string, unknown>;
  }

  private async inspect(operation: string, projectFiles = false): Promise<Record<string, unknown>> {
    return this.command(["pi", "approvals", "--session", this.options.session, "--operation", operation, "--json",
      ...(projectFiles ? ["--project-files"] : [])]);
  }

  private async decide(operation: string, digest: string, decision: "approve" | "decline"): Promise<Record<string, unknown>> {
    return this.command(["pi", "approve", "--session", this.options.session, "--operation", operation,
      "--digest", digest, "--decision", decision]);
  }

  consumeJsonLine(line: string): void {
    const event: unknown = JSON.parse(line);
    if (!isJsonRecord(event)) return;
    if (event.type !== "tool_execution_update") return;
    const update = isJsonRecord(event.partialResult) ? event.partialResult : undefined;
    const descriptor = update?.details;
    if (!isJsonRecord(descriptor) || descriptor.customType !== preparedType) return;
    assert.ok(typeof descriptor.operation === "string");
    assert.equal(event.toolName, "acyclic_writeback");
    assert.equal(event.toolCallId, descriptor.operation, "prepared event changed its original SDK tool identity");
    this.approvePrepared(descriptor.operation);
  }

  private approvePrepared(operation: string): Promise<Record<string, unknown>> {
    assert.equal(operation, this.options.operation);
    const existing = this.decisions.get(operation);
    if (existing) return existing;
    const decision = this.auditAndApprove(operation);
    this.decisions.set(operation, decision);
    void decision.catch(() => {});
    return decision;
  }

  private physical(root: string, path: string): string | null {
    const absolute = resolve(root, path);
    const selected = relative(root, absolute);
    assert.ok(selected && selected !== ".." && !selected.startsWith(`..${process.platform === "win32" ? "\\" : "/"}`) && !isAbsolute(selected));
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

  private async auditAndApprove(operation: string): Promise<Record<string, unknown>> {
    const inspection = await this.inspect(operation, true);
    assert.equal(inspection.operation, operation);
    assert.equal(inspection.status, "prepared");
    assert.equal(typeof inspection.approval_digest, "string");
    assert.deepEqual(inspection.paths, Object.keys(this.options.selected));
    const prepared = inspection.prepared;
    assert.ok(isJsonRecord(prepared) && typeof prepared.root_path === "string");
    assert.equal(realpathSync(prepared.root_path), realpathSync(this.options.cwd));
    assert.equal(prepared.session, this.options.session);
    assert.equal(inspection.file_evidence_complete, true);
    assert.equal(inspection.file_evidence_cursor, null);
    const importedFiles = inspection.imported_selected_files;
    assert.ok(isJsonRecord(importedFiles));
    assert.deepEqual(Object.keys(importedFiles).sort(), Object.keys(this.options.selected).sort());
    for (const [path, oracle] of Object.entries(this.options.selected)) {
      assert.equal(this.physical(this.options.cwd, path), oracle.current);
      const encoded = importedFiles[path];
      if (oracle.candidate === null) assert.equal(encoded, null);
      else {
        assert.ok(typeof encoded === "string" && encoded.length <= 2 * 1024 * 1024);
        const bytes = Buffer.from(encoded, "base64");
        assert.equal(bytes.toString("base64"), encoded);
        assert.deepEqual(bytes, Buffer.from(oracle.candidate, "utf8"));
      }
    }
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
    await this.wrongDigest(operation);
    return this.decide(operation, String(inspection.approval_digest), "approve");
  }

  async assertPublished(): Promise<void> {
    assert.ok(this.decisions.size > 0);
    await Promise.all(this.decisions.values());
    for (const operation of this.decisions.keys()) {
      const inspection = await this.inspect(operation);
      assert.equal(inspection.status, "published");
      assert.equal(inspection.operation, operation);
      assert.ok(isJsonRecord(inspection.receipt));
      assert.equal(inspection.receipt.phase, "Applied", "operator decision is not a native publication receipt");
    }
    for (const [path, oracle] of Object.entries(this.options.selected)) assert.equal(this.physical(this.options.cwd, path), oracle.candidate);
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
  }

  private assertRootUnpublished(): void {
    for (const [path, oracle] of Object.entries(this.options.selected)) assert.equal(this.physical(this.options.cwd, path), oracle.current);
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
  }

  private async wrongDigest(operation: string): Promise<void> {
    const before = await this.inspect(operation);
    this.assertRootUnpublished();
    const digest = String(before.approval_digest);
    const wrong = `${digest[0] === "0" ? "1" : "0"}${digest.slice(1)}`;
    await assert.rejects(this.decide(operation, wrong, "approve"));
    const after = await this.inspect(operation);
    assert.equal(after.status, "prepared");
    assert.equal(after.approval_digest, digest);
    assert.deepEqual(after.prepared, before.prepared, "rejected digest changed the original prepared ticket");
    this.assertRootUnpublished();
  }

}
