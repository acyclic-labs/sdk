import {readFileSync, writeFileSync} from 'node:fs';

const [output, header, ...files] = process.argv.slice(2);
const readJson = file => JSON.parse(readFileSync(file, 'utf8'));
const shards = files.map(readJson).sort((a, b) => a.package.name.localeCompare(b.package.name, 'en'));
const packages = shards.map(shard => shard.package);

writeFileSync(output, JSON.stringify({
  packages,
  workspace_members: packages.map(pkg => pkg.id),
  workspace_default_members: shards.filter(shard => shard.defaultMember).map(shard => shard.package.id),
  resolve: null,
  target_directory: 'target',
  workspace_root: '.',
  ...readJson(header),
}) + '\n');
