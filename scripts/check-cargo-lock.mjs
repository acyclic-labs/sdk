import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';

try {
  const before = readFileSync('Cargo.lock');
  // --locked checks resolution, but accepts serialization Cargo would rewrite.
  // Resolve and serialize without fetching every dependency's source for metadata.
  execFileSync('cargo', ['update', '--workspace'], {stdio: ['ignore', 'ignore', 'inherit']});
  if (!readFileSync('Cargo.lock').equals(before)) {
    throw new Error('Cargo.lock differs from Cargo\'s canonical output');
  }
} catch (error) {
  console.error(`step=cargo-lock-canonical: ${error.message}; recovery command: cargo update --workspace && git add Cargo.lock`);
  process.exitCode = 1;
}
