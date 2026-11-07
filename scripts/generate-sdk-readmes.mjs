import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const services = ["Objects", "Streams", "Actors", "Workers", "fs"];

const root = fileURLToPath(new URL("../", import.meta.url));
const recovery = "node scripts/generate-sdk-readmes.mjs write";

function renderExample(source, code, language) {
  const block = new RegExp(`^\`\`\`${language}\\n[\\s\\S]*?^\`\`\`$`, "gm");
  const matches = [...source.matchAll(block)];
  if (matches.length > 1) throw new Error(`STEP_FAILED README generation: duplicate ${language} example; recovery: ${recovery}`);
  const example = `\`\`\`${language}\n${code}\`\`\``;
  return matches.length ? source.replace(block, () => example) : `${source.trimEnd()}\n\n${example}\n\n`;
}

export function generatedReadmes(read) {
  const outputs = new Map();
  for (const [extension, language, guidePaths, workerPath] of [
    ["rs", "rust,no_run", ["rust/README.md"], "rust/crates/workers/README.md"],
    ["mts", "ts", ["typescript/packages/sdk/README.md", "llms.txt"], "typescript/packages/workers/README.md"],
  ]) {
    const snippets = new Map(services.map(name => [name,
      read(`examples/sdk-services/examples/${name.toLowerCase()}.${extension}`).replaceAll("\r\n", "\n"),
    ]));
    for (const file of [...guidePaths, workerPath]) {
      const source = read(file).replaceAll("\r\n", "\n");
      const generated = file === workerPath ? renderExample(source, snippets.get("Workers"), language)
        : source.split(/(?=^## )/m).map(section => {
          const name = /^## ([^\n]+)\n/.exec(section)?.[1];
          return name && snippets.has(name) ? renderExample(section, snippets.get(name), language) : section;
        }).join("");
      outputs.set(file, { source, generated: `${generated.trimEnd()}\n` });
    }
  }
  return outputs;
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
