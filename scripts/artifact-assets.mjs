import crypto from "node:crypto";
import fs from "node:fs";
import { basename } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const reference = /new URL\(\s*(["'])([^"'\r\n]+\.wasm)\1,\s*import\.meta\.url\s*\)/g;
const rewrite = (text, resolve) => text.replace(reference,
  (_, _quote, path) => `new URL(${JSON.stringify(resolve(path))}, import.meta.url)`);

/** Preserve each importer's exact asset identity across bundling, then seal only
 * references surviving the bundle. One build owns each file snapshot and digest. */
export function wasmAssetClosure(recovery) {
  const snapshots = new Map();
  const files = new Map();
  const assets = new Map();
  return {
    files,
    assets,
    plugin: {
      name: "declared-wasm-assets",
      setup(build) {
        build.onLoad({ filter: /\.[cm]?js$/, namespace: "file" }, ({ path, loader }) => ({
          loader,
          contents: rewrite(fs.readFileSync(path, "utf8"),
            asset => new URL(asset, pathToFileURL(path)).href),
        }));
      },
    },
    rewrite(bundle) {
      return rewrite(bundle, source => {
        try {
          const path = fileURLToPath(new URL(source));
          let asset = snapshots.get(path);
          if (!asset) {
            const bytes = fs.readFileSync(path);
            const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
            asset = { path: `${basename(path, ".wasm")}.${sha256}.wasm`,
              sha256, size: bytes.length, mediaType: "application/wasm", role: "module" };
            snapshots.set(path, asset);
            files.set(asset.path, bytes);
            assets.set(asset.path, asset);
          }
          return `./${asset.path}`;
        } catch (cause) {
          throw new Error(`STEP artifact-assets failed: ${source}; recovery: ${recovery}`, { cause });
        }
      });
    },
  };
}
