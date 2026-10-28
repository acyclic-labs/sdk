import { createHash } from "node:crypto";
import { open } from "node:fs/promises";
import { Transform } from "node:stream";
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

export async function readTarGzip(path, visit, options = /** @type {{ algorithm?: string, encoding?: import("node:crypto").BinaryToTextEncoding }} */ ({})) {
  const { algorithm = "sha256", encoding = "hex" } = options;
  const file = await open(path, "r");
  let source;
  let expanded;
  let finished;
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || before.size === 0n) throw new Error("archive must be a nonempty regular file");
    const hash = createHash(algorithm);
    let bytes = 0n;
    source = file.createReadStream({ autoClose: false });
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
    const after = await file.stat({ bigint: true });
    if (bytes !== before.size || after.size !== before.size || after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs) {
      throw new Error("archive changed while it was being read");
    }
    if (bytes > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("archive length is not representable");
    return { digest: hash.digest(encoding), size: Number(bytes) };
  } finally {
    source?.destroy();
    expanded?.destroy();
    if (finished) await finished.catch(() => {});
    await file.close();
  }
}
