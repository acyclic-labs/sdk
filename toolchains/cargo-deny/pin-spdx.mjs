// Refresh the compact SPDX identifier lists from the pinned upstream commit.
import {createHash} from 'node:crypto';
import {writeFileSync} from 'node:fs';

const commit = 'd46e94e2c78ceede1cfc63cfa0396472d2798d4c';
const data = {commit};
for (const [file, list, key] of [['licenses', 'licenses', 'licenseId'], ['exceptions', 'exceptions', 'licenseExceptionId']]) {
  const response = await fetch(`https://raw.githubusercontent.com/spdx/license-list-data/${commit}/json/${file}.json`);
  if (!response.ok) throw Error(`${file}: HTTP ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer()), parsed = JSON.parse(bytes);
  if (parsed.licenseListVersion !== '3.27.0' || !Array.isArray(parsed[list])) throw Error(`invalid SPDX ${file}`);
  data[file] = [...new Set(parsed[list].map(item => {
    if (typeof item[key] !== 'string' || !item[key]) throw Error(`invalid SPDX identifier in ${file}`);
    return item[key];
  }))].sort();
  data[`${file}Sha256`] = createHash('sha256').update(bytes).digest('hex');
}
writeFileSync('toolchains/cargo-deny/spdx-identifiers.json', JSON.stringify(data) + '\n');
