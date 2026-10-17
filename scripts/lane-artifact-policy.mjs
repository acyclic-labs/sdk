import {readFileSync} from 'node:fs';

// New product documentation requires explicit registration in the same diff.
const productDocs = new Set(JSON.parse(readFileSync(
  new URL('./product-documentation.json', import.meta.url), 'utf8',
)));

export function laneArtifactViolation(path) {
  const lower = path.toLowerCase();
  const name = lower.slice(lower.lastIndexOf('/') + 1);
  // These are test inputs, not lane artifacts. Keep this exception scoped to
  // actual Rust fixture/corpus directories, never a general "test" exemption.
  if (/^rust\/(?:crates\/[^/]+|fuzz\/[^/]+)\/(?:tests\/fixtures|fuzz\/corpus|corpus)\//.test(path)) return;
  if (/(?:^|\/)(?:scratch|scratch-notes|lane-logs|lane-artifacts|study-classes-evidence)(?:\/|$)/.test(lower)
      || /(?:^|\/)rev-\d+[^/]*verdict[^/]*$/.test(lower)
      || /(?:^|[-_.])(?:verdict|lane-log|build-receipt|scratch-notes)(?:[-_.]|$)/.test(name)
      || /\.log$/.test(lower)) return 'lane artifacts belong outside the product repository';
  if (!/\.(?:md|markdown|mdown)$/i.test(path)) return;
  if (productDocs.has(path)) return;
  return 'Markdown path is outside the product documentation and fixture allowlist';
}
