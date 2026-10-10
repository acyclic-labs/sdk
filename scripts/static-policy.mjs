import YAML from './vendor/yaml.cjs';

/** @param {unknown} value */
const mapping = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  ? /** @type {Record<string, unknown>} */ (value) : undefined;

// Parse actual workflow and composite-action mappings. Script strings,
// comments and ordinary values never become action references.
export function pinnedActions(file, source) {
  const document = YAML.parseDocument(source, {merge: true});
  if (document.errors.length) throw new Error(file + ': ' + document.errors[0].message);
  const workflow = mapping(document.toJS({maxAliasCount: 100}));
  const action = value => {
    if (typeof value !== 'string' || (!value.startsWith('./') && !/^[^@]+@[0-9a-f]{40}$/.test(value))) {
      throw new Error(file + ': action must be pinned: ' + String(value));
    }
  };
  /** @param {unknown} items */
  const steps = items => {
    if (!Array.isArray(items)) return;
    for (const value of items) {
      const step = mapping(value);
      if (step && Object.hasOwn(step, 'uses')) action(step.uses);
    }
  };
  for (const value of Object.values(mapping(workflow?.jobs) ?? {})) {
    const job = mapping(value);
    if (job && Object.hasOwn(job, 'uses')) action(job.uses);
    steps(job?.steps);
  }
  steps(mapping(workflow?.runs)?.steps);
}

export const rustFormatNeeded = changed => changed.some(file => file.endsWith('.rs') ||
  /(?:^|\/)\.?rustfmt\.toml$/.test(file) || file === 'rust-toolchain.toml');
