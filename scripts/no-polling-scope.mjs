import {supportedSource} from './source-policy/index.mjs';

export function sourceInScope(file) {
 // Exact dispositions are declarative policy, never executable source.
 return file !== 'scripts/no-polling-dispositions.json' && supportedSource(file)
  && (file === 'package.json' || /^(?:rust\/crates\/|typescript\/packages\/|scripts\/|plugin\/)/.test(file));
}
