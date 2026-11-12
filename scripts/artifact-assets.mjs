import crypto from "node:crypto";
import fs from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
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
  const retain = source => {
    const path = fileURLToPath(new URL(source));
    let asset = snapshots.get(path);
    if (!asset) {
      const bytes = fs.readFileSync(path);
      const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
      asset = {path:`${basename(path, ".wasm")}.${sha256}.wasm`,
        sha256, size:bytes.length, mediaType:"application/wasm", role:"module"};
      snapshots.set(path, asset);
      files.set(asset.path, bytes);
      assets.set(asset.path, asset);
    }
    return `./${asset.path}`;
  };
  return {
    files,
    assets,
    plugin: {
      name: "declared-wasm-assets",
      setup(build) {
        build.onResolve({filter:/\.wasm$/}, ({path}) => assets.has(basename(path)) ? {path, external:true} : undefined);
        build.onLoad({ filter: /\.[cm]?js$/, namespace: "file" }, ({ path, loader }) => {
          let contents = fs.readFileSync(path, "utf8");
          // Hosted runtimes precompile declared WASM modules at admission;
          // their guests cannot generate code from raw bytes during restore.
          if (contents.includes('function initSync(')) {
            const module = [...contents.matchAll(reference)][0];
            if (!module) throw Error(`HostedWasmModuleRequired: ${path}`);
            const asset = retain(new URL(module[2], pathToFileURL(path)).href);
            contents = `import compiledWasm from ${JSON.stringify(asset)};\n${contents}\ninitSync({module:compiledWasm});\n`;
          } else if (/[/\\]auth[/\\](?:dist|src)[/\\]wasm\.[cm]?js$/.test(path)) {
            // The generated facade now owns synchronous initialization too.
            contents = "export * from '../generated/wasm/acyclic_auth_wasm.js';";
          }
          return {loader, contents:rewrite(contents, asset => new URL(asset, pathToFileURL(path)).href)};
        });
      },
    },
    rewrite(bundle) {
      return rewrite(bundle, source => {
        try {
          return retain(source);
        } catch (cause) {
          throw new Error(`STEP artifact-assets failed: ${source}; recovery: ${recovery}`, { cause });
        }
      });
    },
  };
}

/** Load the original module snapshots already verified with the artifact.
 * The loader cannot open undeclared files or observe later file changes. */
export async function loadHostedArtifact(entry, assets, files) {
  const {plugin} = await import('bun');
  const modules = new Map(assets.map(asset => [join(dirname(entry), asset.path), files.get(asset.path)]));
  const directory = dirname(entry)+sep;
  const namespace = 'hosted-wasm-'+crypto.createHash('sha256').update(entry).digest('hex');
  plugin({name:'hosted-artifact-modules', setup(build) {
    build.onResolve({filter:/\.wasm$/}, ({path, importer}) => {
      if (!importer.startsWith(directory)) return;
      const selected = resolve(dirname(importer), path);
      if (!modules.has(selected)) throw Error('HostedWasmAssetUndeclared');
      return {path:selected, namespace};
    });
    build.onLoad({filter:/.*/, namespace}, ({path}) => {
      return {loader:'object', exports:{default:new WebAssembly.Module(modules.get(path))}};
    });
  }});
  return import(pathToFileURL(entry).href);
}
