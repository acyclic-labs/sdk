// Generate a compact index directly from the pinned RustSec tarball. This runs
// as a build action; the test receives only the resulting file.
import {readFileSync, writeFileSync} from 'node:fs';
import {gunzipSync} from 'node:zlib';
import {ranges} from './advisory-ranges.mjs';

const [archive, registryFile, output] = process.argv.slice(2);
if (!archive || !registryFile || !output) throw Error('expected advisory archive, registry state and output path');
const tar = gunzipSync(readFileSync(archive));
const crates = Object.create(null);
for (let offset = 0; offset + 512 <= tar.length;) {
  const header = tar.subarray(offset, offset + 512);
  if (header.every(byte => byte === 0)) break;
  const text = (start, end) => header.subarray(start, end).toString('utf8').replace(/\0.*$/s, '');
  const name = text(0, 100), sizeText = text(124, 136).trim();
  if (!/^[0-7]+$/.test(sizeText)) throw Error(`invalid tar entry size: ${name}`);
  const size = Number.parseInt(sizeText, 8), data = tar.subarray(offset + 512, offset + 512 + size);
  if (data.length !== size) throw Error(`truncated tar entry: ${name}`);
  offset += 512 + Math.ceil(size / 512) * 512;
  if (!/^[^/]+\/crates\/[^/]+\/RUSTSEC-[0-9-]+\.md$/.test(name)) continue;
  const source = data.toString('utf8');
  const block = /^```toml\r?\n([\s\S]*?)\r?\n```/u.exec(source);
  if (!block) throw Error(`missing TOML advisory header: ${name}`);
  const sections = Object.create(null); let section = '';
  for (const line of block[1].split(/\r?\n/)) {
    const heading = /^\s*\[([^\]]+)\]\s*(?:#.*)?$/.exec(line);
    if (heading) section = heading[1];
    else if (section === 'advisory' || section === 'versions') sections[section] = (sections[section] ?? '') + line + '\n';
  }
  const scalar = (sectionName, key, required = false) => {
    const match = new RegExp(`^\\s*${key}\\s*=\\s*"([^"\\\\]*)"\\s*(?:#.*)?$`, 'm').exec(sections[sectionName] ?? '');
    if (!match && required) throw Error(`missing ${sectionName}.${key}: ${name}`);
    return match?.[1];
  };
  const id = scalar('advisory', 'id', true), crate = scalar('advisory', 'package', true);
  if (!name.endsWith(`/${id}.md`) || name.split('/')[2] !== crate) throw Error(`advisory identity mismatch: ${name}`);
  if (scalar('advisory', 'withdrawn')) continue;
  (crates[crate] ??= []).push({id, informational: scalar('advisory', 'informational') ?? null, patched: ranges(sections.versions ?? '', 'patched', name), unaffected: ranges(sections.versions ?? '', 'unaffected', name)});
}
if (!Object.keys(crates).length) throw Error('pinned archive contains no crate advisories');
for (const entries of Object.values(crates)) entries.sort((a, b) => a.id.localeCompare(b.id, 'en'));
const registry = JSON.parse(readFileSync(registryFile, 'utf8'));
if (!/^[0-9a-f]{40}$/.test(registry.commit) || !registry.crates || !Object.keys(registry.crates).length) throw Error('invalid pinned registry state');
writeFileSync(output, JSON.stringify({advisories: Object.fromEntries(Object.entries(crates).sort(([a], [b]) => a.localeCompare(b, 'en'))), registry}) + '\n');
