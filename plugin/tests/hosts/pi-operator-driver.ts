import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { lstatSync, readFileSync, realpathSync } from "node:fs";
import { isAbsolute, relative, resolve } from "node:path";
import { promisify } from "node:util";
import { blake3 } from "@noble/hashes/blake3";

const execute = promisify(execFile);
const preparedType = "acyclic.writeback.prepared";

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}


export interface OperatorFileOracle {
  baseline: string | null;
  candidate: string | null;
  current: string | null;
}
export interface PiOperatorDriverOptions {
  binary: string;
  session: string;
  cwd: string;
  environment?: NodeJS.ProcessEnv;
  command?: (args: string[], input?: string) => Promise<{ stdout: string; stderr: string }>;
  selected: Readonly<Record<string, OperatorFileOracle>>;
  preserved: Readonly<Record<string, string>>;
  operation?: string;
  beforeApprove?: (driver: PiOperatorDriver, operation: string) => Promise<void>;
}

/** Outside-model driver: only the real protected-root CLI may decide a ticket. */
export class PiOperatorDriver {
  readonly options: PiOperatorDriverOptions;
  private readonly decisions = new Map<string, Promise<Record<string, unknown>>>();

  constructor(options: PiOperatorDriverOptions) {
    assert.ok(isAbsolute(options.binary), "operator must use the final absolute ACYCLIC_BINARY");
    assert.equal(realpathSync(options.binary), realpathSync(process.env.ACYCLIC_BINARY ?? options.binary));
    assert.ok(isAbsolute(options.cwd));
    assert.ok(options.session);
    assert.ok(Object.keys(options.selected).length > 0);
    this.options = options;
    // Construct before starting the original SDK turn, so the baseline is an
    // independent physical observation, never a value copied from model output.
    for (const [path, oracle] of Object.entries(options.selected)) {
      assert.equal(this.physical(options.cwd, path), oracle.baseline, `independent initial Root baseline differs: ${path}`);
    }
  }

  private async command(args: string[], input?: string): Promise<Record<string, unknown>> {
    let stdout: string;
    if (this.options.command) ({ stdout } = await this.options.command(args, input));
    else {
      const invocation = execute(this.options.binary, args, { cwd: this.options.cwd,
        env: this.options.environment, encoding: "utf8", windowsHide: true, timeout: 60_000, maxBuffer: 4 * 1024 * 1024 });
      invocation.child.stdin?.end(input ?? "");
      ({ stdout } = await invocation);
    }
    const result: unknown = JSON.parse(stdout);
    assert.ok(result && typeof result === "object" && !Array.isArray(result));
    return result as Record<string, unknown>;
  }

  async inspect(operation: string, projectFiles = false): Promise<Record<string, unknown>> {
    return this.command(["pi", "approvals", "--session", this.options.session, "--operation", operation, "--json",
      ...(projectFiles ? ["--project-files"] : [])]);
  }

  async decide(operation: string, digest: string, decision: "approve" | "decline"): Promise<Record<string, unknown>> {
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
    // No approval digest or authority is taken from the model's message.
    this.approvePrepared(descriptor.operation);
  }

  approvePrepared(operation: string): Promise<Record<string, unknown>> {
    if (this.options.operation) assert.equal(operation, this.options.operation);
    const existing = this.decisions.get(operation);
    if (existing) return existing;
    const decision = this.auditAndApprove(operation);
    this.decisions.set(operation, decision);
    // The completion barrier owns propagation, including early stream failures.
    void decision.catch(() => {});
    return decision;
  }

  async completed(): Promise<Record<string, unknown>[]> {
    assert.ok(this.decisions.size > 0, "no original prepared SDK operation was observed or configured");
    return Promise.all(this.decisions.values());
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
    // Backend inspection, not the model event, supplies immutable raw bytes.
    const prepared = inspection.prepared;
    assert.ok(isJsonRecord(prepared) && isJsonRecord(prepared.command) && isJsonRecord(prepared.journal));
    assert.ok(typeof prepared.canonical_digest_input === "string");
    const canonicalBytes = Buffer.from(prepared.canonical_digest_input, "utf8");
    assert.ok(canonicalBytes.length <= 1024 * 1024, "operator canonical preparation exceeds its bound");
    const digestInput: unknown = JSON.parse(prepared.canonical_digest_input);
    assert.ok(Array.isArray(digestInput) && digestInput.length === 16);
    const digest = Buffer.from(blake3(canonicalBytes)).toString("hex");
    assert.equal(digest, inspection.approval_digest, "actual canonical preparation digest differs");
    assert.ok(typeof prepared.root_path === "string" && isAbsolute(prepared.root_path));
    assert.equal(realpathSync(prepared.root_path), realpathSync(this.options.cwd));
    assert.ok(typeof prepared.source_path === "string" && isAbsolute(prepared.source_path));
    const journal = prepared.journal;
    assert.equal(journal.phase, "Prepared");
    assert.equal(prepared.session, this.options.session);
    assert.equal(prepared.command.sdk_operation, operation);
    assert.equal(prepared.command.source, inspection.source);
    assert.equal(prepared.command.root, inspection.root);
    assert.deepEqual(prepared.command.paths, inspection.paths);
    assert.equal(digestInput[0], prepared.command.caller);
    assert.notEqual(prepared.command.source, prepared.command.caller, "candidate must come from the separately selected child");
    assert.deepEqual(digestInput[3], journal.root_identity, "actual prepared root differs from the original root identity");
    assert.ok(isJsonRecord(journal.preparation_identity), "actual native staging allocation is unwitnessed");
    const originalSnapshot: unknown = digestInput[10];
    assert.ok(isJsonRecord(originalSnapshot) && isJsonRecord(prepared.selected_snapshot));
    assert.deepEqual(prepared.source_snapshot, originalSnapshot, "original Root baseline differs from the canonical digest");
    assert.equal(originalSnapshot.workspace_id, digestInput[2], "original baseline belongs to another root workspace");
    assert.notEqual(prepared.selected_snapshot.workspace_id, originalSnapshot.workspace_id,
      "selected source must retain its independent child workspace identity");
    const importReceipt = prepared.import_receipt;
    assert.ok(isJsonRecord(importReceipt) && isJsonRecord(importReceipt.imported) &&
      isJsonRecord(importReceipt.destination_snapshot));
    assert.equal(importReceipt.caller, prepared.command.caller);
    assert.equal(importReceipt.source, prepared.command.source);
    assert.equal(importReceipt.root, prepared.command.root);
    assert.deepEqual(importReceipt.paths, prepared.command.paths);
    assert.deepEqual(importReceipt.selected_snapshot, prepared.selected_snapshot);
    assert.deepEqual(importReceipt.destination, digestInput[1]);
    assert.equal(importReceipt.destination_snapshot.workspace_id, originalSnapshot.workspace_id);
    assert.equal(importReceipt.imported.workspace_id, originalSnapshot.workspace_id);
    assert.equal(importReceipt.imported.authored_generation, prepared.imported);
    assert.equal(digestInput[4], prepared.root_path);
    assert.ok(typeof digestInput[5] === "string" && isAbsolute(digestInput[5]));
    const targetPath = resolve(digestInput[5], "target");
    assert.equal(digestInput[6], prepared.operation);
    assert.deepEqual(digestInput[9], inspection.paths);
    assert.equal(digestInput[11], prepared.imported);
    assert.deepEqual(digestInput[12], journal.plan);
    assert.deepEqual(digestInput[13], journal.root_identity);
    assert.deepEqual(digestInput[14], journal.preparation_identity);
    assert.deepEqual(digestInput[15], journal.preimages);
    assert.ok(isJsonRecord(journal.plan) && isJsonRecord(journal.plan.intent));
    assert.equal(journal.plan.operation_id, prepared.operation);
    const transition = journal.plan.intent.GenerationTransition;
    assert.ok(isJsonRecord(transition));
    assert.equal(transition.from, digestInput[7]);
    assert.equal(transition.to, digestInput[8]);
    assert.notEqual(transition.from, transition.to, "writeback must have an actual candidate generation");
    assert.ok(Array.isArray(journal.plan.edits) && Array.isArray(journal.preimages));
    assert.equal(journal.plan.edits.length, journal.preimages.length);
    const edited = new Set<string>();
    for (let index = 0; index < journal.plan.edits.length; index++) {
      const edit: unknown = journal.plan.edits[index];
      const preimage: unknown = journal.preimages[index];
      assert.ok(isJsonRecord(edit) && isJsonRecord(preimage) && Array.isArray(preimage.image));
      const install = edit.Install;
      const remove = edit.Remove;
      assert.ok(isJsonRecord(install) || isJsonRecord(remove), "oracle only authorizes explicit regular-file installs/removals");
      const path = isJsonRecord(install) ? install.path : isJsonRecord(remove) ? remove.path : undefined;
      assert.ok(typeof path === "string" && Object.hasOwn(this.options.selected, path));
      assert.ok(!edited.has(path), "prepared candidate repeats a selected edit");
      edited.add(path);
      const oracle = this.options.selected[path];
      assert.equal(isJsonRecord(install), oracle.candidate !== null);
      assert.ok(preimage.image.length >= 67 && preimage.image.every(byte => Number.isInteger(byte) && byte >= 0 && byte <= 255));
      assert.equal(preimage.image[0], 2, "unexpected actual native witness version");
      assert.equal(preimage.image[1], oracle.current === null ? 0 : 1, "native captured preimage presence differs");
      assert.equal(preimage.image[34], oracle.candidate === null ? 0 : 1, "native captured candidate presence differs");
      if (oracle.current === null) assert.deepEqual(preimage.image.slice(2, 34), Array(32).fill(0));
      assert.equal(this.physical(targetPath, path), oracle.candidate, `actual staged native candidate differs: ${path}`);
    }
    assert.deepEqual([...edited].sort(), Object.keys(this.options.selected).sort());
    assert.deepEqual(inspection.paths, Object.keys(this.options.selected));
    assert.equal(inspection.file_evidence_complete, true, "independent fixture oracle requires its whole bounded evidence page");
    assert.equal(inspection.file_evidence_cursor, null, "partial imported bytes cannot authorize publication");
    const importedFiles = inspection.imported_selected_files;
    assert.ok(isJsonRecord(importedFiles));
    assert.deepEqual(Object.keys(importedFiles).sort(), Object.keys(this.options.selected).sort());
    for (const [path, oracle] of Object.entries(this.options.selected)) {
      assert.equal(this.physical(this.options.cwd, path), oracle.current, `current original Root differs: ${path}`);
      // Terminal native mounts may be disposed. These bytes come from the
      // exact Root-owned imported SDK generation bound to the original source
      // seal and receipt, never a guessed path or fresh child capture.
      const encoded = importedFiles[path];
      if (oracle.candidate === null) assert.equal(encoded, null);
      else {
        assert.ok(typeof encoded === "string" && encoded.length <= 2 * 1024 * 1024);
        const bytes = Buffer.from(encoded, "base64");
        assert.equal(bytes.toString("base64"), encoded, "imported file bytes are not canonical base64");
        assert.deepEqual(bytes, Buffer.from(oracle.candidate, "utf8"), `immutable imported SDK candidate differs: ${path}`);
      }
    }
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
    await this.options.beforeApprove?.(this, operation);
    return this.decide(operation, digest, "approve");
  }

  async assertPublished(): Promise<void> {
    await this.completed();
    for (const operation of this.decisions.keys()) {
      const inspection = await this.inspect(operation);
      assert.equal(inspection.status, "published");
      assert.equal(inspection.operation, operation);
      assert.ok(isJsonRecord(inspection.receipt) && isJsonRecord(inspection.prepared) &&
        isJsonRecord(inspection.prepared.journal));
      assert.equal(inspection.receipt.phase, "Applied", "operator decision is not a native publication receipt");
      assert.deepEqual(inspection.receipt.plan, inspection.prepared.journal.plan);
      assert.deepEqual(inspection.receipt.preimages, inspection.prepared.journal.preimages);
      assert.deepEqual(inspection.receipt.root_identity, inspection.prepared.journal.root_identity);
      assert.deepEqual(inspection.receipt.preparation_identity, inspection.prepared.journal.preparation_identity);
    }
    for (const [path, oracle] of Object.entries(this.options.selected)) assert.equal(this.physical(this.options.cwd, path), oracle.candidate);
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
  }

  private assertRootUnpublished(): void {
    for (const [path, oracle] of Object.entries(this.options.selected)) assert.equal(this.physical(this.options.cwd, path), oracle.current);
    for (const [path, contents] of Object.entries(this.options.preserved)) assert.equal(this.physical(this.options.cwd, path), contents);
  }

  async wrongDigest(operation: string): Promise<void> {
    const before = await this.inspect(operation);
    this.assertRootUnpublished();
    const digest = String(before.approval_digest);
    const wrong = `${digest[0] === "0" ? "1" : "0"}${digest.slice(1)}`;
    await assert.rejects(this.decide(operation, wrong, "approve"),
      error => error instanceof Error && /wrong prepared digest/.test(error.message));
    const after = await this.inspect(operation);
    assert.equal(after.status, "prepared");
    assert.equal(after.approval_digest, digest);
    assert.deepEqual(after.prepared, before.prepared, "rejected digest changed the original prepared ticket");
    this.assertRootUnpublished();
  }

  async decline(operation: string): Promise<Record<string, unknown>> {
    const inspection = await this.inspect(operation);
    this.assertRootUnpublished();
    const result = await this.decide(operation, String(inspection.approval_digest), "decline");
    assert.equal((await this.inspect(operation)).status, "declined");
    this.assertRootUnpublished();
    return result;
  }

  async foreignOperation(operation: string, foreign: string): Promise<void> {
    const inspection = await this.inspect(operation);
    this.assertRootUnpublished();
    await assert.rejects(this.decide(foreign, String(inspection.approval_digest), "approve"),
      error => error instanceof Error && /original operation has no prepared approval|wrong prepared digest/.test(error.message));
    const after = await this.inspect(operation);
    assert.equal(after.status, "prepared");
    assert.deepEqual(after.prepared, inspection.prepared, "foreign operation changed the original prepared ticket");
    this.assertRootUnpublished();
  }

  async foreignCaller(operation: string, childProof: unknown): Promise<void> {
    const inspection = await this.inspect(operation);
    assert.ok(isJsonRecord(childProof), "foreign caller control requires its actual signed child proof");
    this.assertRootUnpublished();
    await assert.rejects(this.command(["__pi", "OperatorApproval"], JSON.stringify({
      session_id: this.options.session, operator: childProof,
      body: { action: "approval", prepared: inspection.prepared, digest: inspection.approval_digest, decision: "approve" },
    })), error => error instanceof Error && /operator response differs from its original protected Root and exact body/.test(error.message));
    const after = await this.inspect(operation);
    assert.equal(after.status, "prepared");
    assert.deepEqual(after.prepared, inspection.prepared, "foreign caller changed the original prepared ticket");
    this.assertRootUnpublished();
  }
}
