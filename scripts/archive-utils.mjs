import { constants } from "node:fs";
import { createHash } from "node:crypto";
import { cpSync, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, statSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { copyFile, open } from "node:fs/promises";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";

export function sha256(payload) {
  return createHash("sha256").update(payload).digest("hex");
}

// Retain only the current decompressor chunk and fixed-size tar header.
// Entry consumers choose which bodies to retain; every other body is drained.
class ChunkReader {
  constructor(chunks) {
    this.iterator = chunks[Symbol.asyncIterator]?.() ?? chunks[Symbol.iterator]();
    this.chunk = Buffer.alloc(0);
    this.offset = 0;
    this.position = 0;
  }

  async piece(maximum) {
    while (this.offset === this.chunk.length) {
      const next = await this.iterator.next();
      if (next.done) return null;
      this.chunk = next.value;
      this.offset = 0;
    }
    const length = Math.min(maximum, this.chunk.length - this.offset);
    const result = this.chunk.subarray(this.offset, this.offset + length);
    this.offset += length;
    this.position += length;
    if (!Number.isSafeInteger(this.position)) throw new Error("archive offset is not representable");
    return result;
  }

  async header() {
    const header = Buffer.allocUnsafe(512);
    let offset = 0;
    while (offset < header.length) {
      const chunk = await this.piece(header.length - offset);
      if (!chunk) throw new Error("archive is truncated or lacks an end marker");
      header.set(chunk, offset);
      offset += chunk.length;
    }
    return header;
  }

  async skip(length) {
    while (length > 0) {
      const chunk = await this.piece(length);
      if (!chunk) throw new Error("archive member is truncated");
      length -= chunk.length;
    }
  }
}

function field(block, offset, length) {
  const end = block.indexOf(0, offset);
  return block.subarray(offset, end === -1 || end > offset + length ? offset + length : end).toString("utf8");
}

export async function visitTar(chunks, visit) {
  const reader = new ChunkReader(chunks);
  try {
    while (true) {
      const header = await reader.header();
      if (header.every(byte => byte === 0)) {
        if (!(await reader.header()).every(byte => byte === 0)) throw new Error("archive has an invalid end marker");
        let chunk;
        while ((chunk = await reader.piece(64 * 1024)) !== null) {
          if (!chunk.every(byte => byte === 0)) throw new Error("archive has an invalid end marker");
        }
        return;
      }
      const rawChecksum = field(header, 148, 8).trim();
      if (!/^[0-7]+$/.test(rawChecksum)) throw new Error("archive member has an invalid checksum");
      const checksum = header.reduce((sum, byte, index) => sum + (index >= 148 && index < 156 ? 32 : byte), 0);
      if (checksum !== Number.parseInt(rawChecksum, 8)) throw new Error("archive member checksum mismatch");
      const name = field(header, 0, 100);
      const prefix = field(header, 345, 155);
      const path = prefix ? `${prefix}/${name}` : name;
      const rawSize = field(header, 124, 12).trim();
      if (rawSize !== "" && !/^[0-7]+$/.test(rawSize)) throw new Error("archive member has an invalid size");
      const size = rawSize === "" ? 0 : Number.parseInt(rawSize, 8);
      const type = String.fromCharCode(header[156] || 48);
      const bodyEnd = reader.position + size;
      const paddedEnd = reader.position + Math.ceil(size / 512) * 512;
      if (!Number.isSafeInteger(size) || size < 0 || !Number.isSafeInteger(paddedEnd)) throw new Error("archive member has an invalid size");
      let active = true;
      const body = (async function* () {
        while (reader.position < bodyEnd) {
          if (!active) throw new Error("archive body must be read during its visit");
          const chunk = await reader.piece(bodyEnd - reader.position);
          if (!chunk) throw new Error("archive member is truncated");
          yield chunk;
        }
      })();
      try {
        await visit({ path, type, size, body });
      } finally {
        active = false;
        await body.return();
      }
      await reader.skip(paddedEnd - reader.position);
    }
  } finally {
    await reader.iterator.return?.();
  }
}

export async function collectBody(entry) {
  const chunks = [];
  for await (const chunk of entry.body) chunks.push(chunk);
  return Buffer.concat(chunks, entry.size);
}

export async function hashBody(entry) {
  const hash = createHash("sha256");
  for await (const chunk of entry.body) hash.update(chunk);
  return hash.digest("hex");
}

export async function readTarGzip(input, visit, options = /** @type {{ algorithm?: string, encoding?: import("node:crypto").BinaryToTextEncoding }} */ ({})) {
  const { algorithm = "sha256", encoding = "hex" } = options;
  const file = input instanceof Uint8Array ? null : await open(input, "r");
  let source;
  let expanded;
  let finished;
  try {
    const before = file ? await file.stat({ bigint: true }) : null;
    if (before ? !before.isFile() || before.size === 0n : input.length === 0) throw new Error("archive must be a nonempty regular file");
    const hash = createHash(algorithm);
    let bytes = 0n;
    source = file ? file.createReadStream({ autoClose: false }) : Readable.from([input]);
    expanded = createGunzip();
    finished = pipeline(source, new Transform({
      transform(chunk, _encoding, callback) {
        hash.update(chunk);
        bytes += BigInt(chunk.length);
        callback(null, chunk);
      },
    }), expanded);
    // The output iterator carries the same error; attach immediately so an
    // input failure cannot become an unhandled rejection before it is awaited.
    finished.catch(() => {});
    await visitTar(expanded, visit);
    await finished;
    const after = file ? await file.stat({ bigint: true }) : null;
    if (before && (bytes !== before.size || after.size !== before.size || after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs)) {
      throw new Error("archive changed while it was being read");
    }
    if (bytes > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("archive length is not representable");
    return { digest: hash.digest(encoding), size: Number(bytes) };
  } finally {
    source?.destroy();
    expanded?.destroy();
    if (finished) await finished.catch(() => {});
    await file?.close();
  }
}

// Validate names before retaining individual files. The expanded tar itself is
// never materialized; exact inventories and digests remain caller authority.
export async function readPayloadEntries(visitEntries, { canonical, caseInsensitive = process.platform === "win32", declared, expected, regularOnly = false, retain = () => true, collectDigests = false } = {}) {
  const payload = new Map(), digests = new Map(), seen = new Set();
  let files = 0;
  const evidence = await visitEntries(async entry => {
    const name = entry.type === "5" ? entry.path.replace(/\/$/, "") : entry.path;
    const alias = caseInsensitive ? name.toLowerCase() : name;
    if (!canonical(name) || seen.has(alias) || (entry.type !== "0" && (declared || regularOnly || entry.type !== "5"))) throw new Error("unsafe or duplicate archive member");
    seen.add(alias);
    if (entry.type === "5") return;
    files++;
    if (declared && !Object.hasOwn(declared, name)) throw new Error("archive inventory differs");
    let digest;
    if (retain(name)) {
      const body = await collectBody(entry);
      if (declared || collectDigests) digest = sha256(body);
      payload.set(name, body);
    } else {
      digest = await hashBody(entry);
    }
    if (declared && digest !== declared[name]) throw new Error("archive payload digest differs");
    if (digest !== undefined) digests.set(name, digest);
  });
  if (expected !== undefined && evidence.digest !== expected) throw new Error("archive digest differs");
  if (!files || (declared && files !== Object.keys(declared).length)) throw new Error("archive inventory differs");
  return { payload, digests, ...evidence };
}

export async function readArchivePayload(input, options = {}) {
  return readPayloadEntries(visit => readTarGzip(input, visit), options);
}

export async function copyVerifiedArchive(source, destination, expected) {
  await copyFile(source, destination, constants.COPYFILE_EXCL);
  if ((await readTarGzip(destination, () => {})).digest !== expected) throw new Error("archive evidence changed");
}

// wasm-bindgen emits flat bindings plus inline JavaScript under snippets/.
export function isWasmArtifactPath(path) {
  const parts = path.split("/");
  return parts.every(part => part !== "" && part !== "." && part !== ".." && !part.includes("\\"))
    && (parts.length === 1 || (parts[0] === "snippets" && parts.length > 1));
}

/** Enumerate the complete emitted tree without following symbolic links. */
function* wasmArtifactPaths(directory, { receipt = false } = {}) {
  const root = lstatSync(directory);
  if (!root.isDirectory() || root.isSymbolicLink()) {
    throw new Error(`WASM artifact admission failed at ${directory}: expected a real directory; recovery: bun scripts/build-wasm.mjs`);
  }
  function* visit(parent, prefix) {
    for (const entry of readdirSync(parent, { withFileTypes: true })) {
      const path = prefix + entry.name;
      if (entry.isDirectory() && !entry.isSymbolicLink() && (path === "snippets" || path.startsWith("snippets/"))) {
        yield* visit(join(parent, entry.name), `${path}/`);
      } else if (entry.isFile() && !entry.isSymbolicLink() && isWasmArtifactPath(path)) {
        if (receipt || path !== "producer-receipt.json") yield path;
      } else {
        throw new Error(`WASM artifact admission failed at ${path}: non-regular artifact; recovery: bun scripts/build-wasm.mjs`);
      }
    }
  }
  yield* visit(directory, "");
}

/** @returns {Generator<[string, Buffer]>} */
export function* wasmArtifactFiles(directory, options) {
  for (const path of wasmArtifactPaths(directory, options)) {
    yield [`generated/wasm/${path}`, readFileSync(join(directory, path))];
  }
}

/** Replace only admitted generated files, including snippets from an older build. */
export function copyWasmArtifacts(source, output) {
  const artifacts = wasmArtifactInventory(wasmArtifactFiles(source));
  const current = new Set(artifacts.map(item => item.path.slice("generated/wasm/".length)));
  // Admit the complete old tree before removing anything. Directory entries
  // alone suffice; stale payloads need neither reading nor hashing.
  const previous = existsSync(output) ? [...wasmArtifactPaths(output, { receipt: true })] : [];
  for (const path of previous) if (!current.has(path)) unlinkSync(join(output, path));
  mkdirSync(output, { recursive: true });
  cpSync(source, output, { recursive: true });
  return artifacts;
}

export function wasmArtifactInventory(files) {
  return Array.from(files, ([path, bytes]) => ({
    path, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
  })).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
}
