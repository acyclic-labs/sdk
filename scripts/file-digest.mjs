import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";

/** Hash the complete file without retaining its payload. */
export async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const bytes of createReadStream(path)) hash.update(bytes);
  return hash.digest("hex");
}
