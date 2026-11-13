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

/** Verified module bytes by digest, and each loaded artifact's declared assets.
 * Bun registers loader plugins per process, so one registration serves all. */
const hosted = {modules:new Map(), directories:new Map(), registered:false};

/** Load the original module snapshots already verified with the artifact.
 * The loader cannot open undeclared files or observe later file changes.
 * A module resolves to its content digest, never to the staging path: Bun's
 * transpiler cache replays a resolution for identical artifact bytes, so a
 * location-bound one would name another process's deleted directory. */
export async function loadHostedArtifact(entry, assets, files) {
  const {plugin} = await import('bun');
  const declared = new Map();
  for (const asset of assets) {
    const bytes = files.get(asset.path);
    if (bytes === undefined || crypto.createHash('sha256').update(bytes).digest('hex') !== asset.sha256) {
      throw Error(`HostedWasmAssetUnverified: ${asset.path}`);
    }
    hosted.modules.set(asset.sha256, bytes);
    declared.set(join(dirname(entry), asset.path), asset.sha256);
  }
  hosted.directories.set(dirname(entry)+sep, declared);
  if (!hosted.registered) {
    hosted.registered = true;
    plugin({name:'hosted-artifact-modules', setup(build) {
      build.onResolve({filter:/\.wasm$/}, ({path, importer}) => {
        const directory = [...hosted.directories.keys()].filter(directory => importer.startsWith(directory))
          .reduce((longest, directory) => directory.length > longest.length ? directory : longest, '');
        if (directory === '') return;
        const digest = hosted.directories.get(directory).get(resolve(dirname(importer), path));
        if (digest === undefined) throw Error('HostedWasmAssetUndeclared');
        return {path:digest, namespace:'hosted-wasm'};
      });
      build.onLoad({filter:/.*/, namespace:'hosted-wasm'}, ({path}) => {
        const bytes = hosted.modules.get(path);
        if (bytes === undefined) throw Error('HostedWasmAssetUndeclared');
        return {loader:'object', exports:{default:new WebAssembly.Module(bytes)}};
      });
    }});
  }
  return import(pathToFileURL(entry).href);
}
