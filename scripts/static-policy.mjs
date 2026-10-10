import YAML from './vendor/yaml.cjs';

// Parse actual workflow and composite-action mappings. Script strings,
// comments and ordinary values never become action references.
export function pinnedActions(file, source) {
  const document = YAML.parseDocument(source, {merge: true});
  if (document.errors.length) throw new Error(file + ': ' + document.errors[0].message);
  const workflow = document.toJS({maxAliasCount: 100});
  const action = value => {
    if (typeof value !== 'string' || (!value.startsWith('./') && !/^[^@]+@[0-9a-f]{40}$/.test(value))) {
      throw new Error(file + ': action must be pinned: ' + String(value));
    }
  };
  const steps = items => {
    if (!Array.isArray(items)) return;
    for (const step of items) if (step && Object.hasOwn(step, 'uses')) action(step.uses);
  };
  for (const job of Object.values(workflow?.jobs ?? {})) {
    if (job && Object.hasOwn(job, 'uses')) action(job.uses);
    steps(job?.steps);
  }
  steps(workflow?.runs?.steps);
}

export const rustFormatNeeded = changed => changed.some(file => file.endsWith('.rs') ||
  /(?:^|\/)\.?rustfmt\.toml$/.test(file) || file === 'rust-toolchain.toml');
