import {execFileSync, spawnSync} from 'node:child_process';
import {checks} from './target-features.mjs';

const metadata = JSON.parse(execFileSync('cargo', ['metadata', '--locked', '--offline', '--no-deps', '--format-version', '1'], {encoding: 'utf8', maxBuffer: 1 << 28}));
const commands = checks(metadata);
if (!commands.length) throw new Error('target feature qualification selected no compiler commands');
for (const {package: name, features, args} of commands) {
  console.log(`Target features: ${name} [default${features.length ? `,${features.join(',')}` : ''}]`);
  const result = spawnSync('cargo', args, {stdio: 'inherit'});
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
console.log(`Cargo accepted ${commands.length} declared target feature configurations`);
