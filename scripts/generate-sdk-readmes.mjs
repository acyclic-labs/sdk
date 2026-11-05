import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const services = ["Objects", "Streams", "Actors", "Workers", "fs"];

const root = fileURLToPath(new URL("../", import.meta.url));
const recovery = "node scripts/generate-sdk-readmes.mjs write";

function renderExample(source, code) {
  const block = /^```rust,no_run\n[\s\S]*?^```$/gm;
  const matches = [...source.matchAll(block)];
  if (matches.length > 1) throw new Error(`STEP_FAILED README generation: duplicate Rust example; recovery: ${recovery}`);
  const example = `\`\`\`rust,no_run\n${code}\`\`\``;
  return matches.length ? source.replace(block, () => example) : `${source.trimEnd()}\n\n${example}\n\n`;
}

export function generatedReadmes(read) {
  const snippets = new Map(services.map(name => [name,
    read(`examples/sdk-services/examples/${name.toLowerCase()}.rs`).replaceAll("\r\n", "\n"),
  ]));
  const guide = read("rust/README.md").replaceAll("\r\n", "\n");
  const workers = read("rust/crates/workers/README.md").replaceAll("\r\n", "\n");
  const sections = guide.split(/(?=^## )/m).map(section => {
    const name = /^## ([^\n]+)\n/.exec(section)?.[1];
    return name && snippets.has(name) ? renderExample(section, snippets.get(name)) : section;
  });
  return new Map([
    ["rust/README.md", { source: guide, generated: `${sections.join("").trimEnd()}\n` }],
    ["rust/crates/workers/README.md", { source: workers, generated: renderExample(workers, snippets.get("Workers")) }],
  ]);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const mode = process.argv[2] ?? "check";
  if (mode !== "write" && mode !== "check") throw new Error(`STEP_FAILED README mode: expected write|check; recovery: ${recovery}`);
  const read = file => readFileSync(resolve(root, file), "utf8");
  for (const [file, { source, generated }] of generatedReadmes(read)) {
    if (source === generated) continue;
    if (mode === "write") writeFileSync(resolve(root, file), generated);
    else throw new Error(`STEP_FAILED README drift: ${file}; recovery: ${recovery}`);
  }
}
