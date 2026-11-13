import { createHash } from "node:crypto";
import { open } from "node:fs/promises";

/** Hash one stable regular file without retaining its payload. */
export async function digestFile(path) {
  const file = await open(path, "r");
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile()) throw new Error("digest input is not a regular file");
    const hash = createHash("sha256");
    let size = 0n;
    for await (const chunk of file.createReadStream({ autoClose: false })) {
      hash.update(chunk);
      size += BigInt(chunk.length);
    }
    const after = await file.stat({ bigint: true });
    if (size !== before.size || after.size !== before.size || after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs) throw new Error("digest input changed while reading");
    return { digest: hash.digest("hex"), size };
  } finally {
    await file.close();
  }
}

export async function sha256File(path) {
  return (await digestFile(path)).digest;
}
