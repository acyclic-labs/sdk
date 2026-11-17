// Refresh the compact, checksum-verified view of the pinned crates.io index.
// Run when the resolved registry crates or the index commit change; never at test time.
import {createHash} from 'node:crypto';
import {readFileSync, writeFileSync} from 'node:fs';

const pins = JSON.parse(readFileSync('toolchains/cargo-deny/registry-pins.json', 'utf8'));
const lock = JSON.parse(readFileSync('cargo-bazel-lock.json', 'utf8'));
if (!/^[0-9a-f]{40}$/.test(pins.commit)) throw Error('invalid registry index commit');
const wanted = new Map();
for (const entry of Object.values(lock.crates)) {
  const url = entry.repository?.Http?.url;
  if (!url?.startsWith('https://static.crates.io/crates/')) continue;
  const versions = wanted.get(entry.name) ?? new Map();
  versions.set(entry.version, entry.repository.Http.sha256);
  wanted.set(entry.name, versions);
}
const indexPath = name => name.length === 1 ? `1/${name}` : name.length === 2 ? `2/${name}` : name.length === 3 ? `3/${name[0]}/${name}` : `${name.slice(0,2)}/${name.slice(2,4)}/${name}`;
const names = [...wanted.keys()].sort(), found = new Map(); let next = 0;
await Promise.all(Array.from({length: 16}, async () => {
  while (next < names.length) {
    const name = names[next++], expected = pins.packages[name];
    if (expected && !/^[0-9a-f]{64}$/.test(expected)) throw Error(`invalid index pin for ${name}`);
    const response = await fetch(`https://raw.githubusercontent.com/rust-lang/crates.io-index/${pins.commit}/${indexPath(name)}`);
    if (!response.ok) throw Error(`${name}: index download returned ${response.status}`);
    const bytes = Buffer.from(await response.arrayBuffer());
    const digest = createHash('sha256').update(bytes).digest('hex');
    if (expected && digest !== expected) throw Error(`${name}: index checksum mismatch`);
    pins.packages[name] = digest;
    const versions = wanted.get(name), records = Object.create(null);
    for (const line of bytes.toString('utf8').trim().split('\n')) {
      const entry = JSON.parse(line);
      if (!versions.has(entry.vers)) continue;
      if (entry.name !== name || entry.cksum !== versions.get(entry.vers) || typeof entry.yanked !== 'boolean') throw Error(`${name} ${entry.vers}: invalid index entry`);
      records[entry.vers] = entry.yanked;
    }
    for (const version of versions.keys()) if (!(version in records)) throw Error(`${name} ${version}: missing pinned index entry`);
    found.set(name, records);
  }
}));
writeFileSync('toolchains/cargo-deny/registry-pins.json', JSON.stringify({commit: pins.commit, packages: Object.fromEntries(Object.entries(pins.packages).sort(([a], [b]) => a.localeCompare(b, 'en')))}) + '\n');
writeFileSync('toolchains/cargo-deny/registry-state.json', JSON.stringify({commit: pins.commit, crates: Object.fromEntries(names.map(name => [name, found.get(name)]))}) + '\n');
