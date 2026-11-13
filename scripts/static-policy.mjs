import YAML from './vendor/yaml.cjs';

/** @param {unknown} value */
const mapping = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  ? /** @type {Record<string, unknown>} */ (value) : undefined;

// Parse actual workflow and composite-action mappings. Script strings,
// comments and ordinary values never become action references.
function workflowMapping(file, source) {
  const document = YAML.parseDocument(source, {merge: true});
  if (document.errors.length) throw new Error(file + ': ' + document.errors[0].message);
  const workflow = mapping(document.toJS({maxAliasCount: 100}));
  if (!workflow) throw new Error(file + ': expected a workflow mapping');
  return workflow;
}

export function pinnedActions(file, source) {
  const workflow = workflowMapping(file, source);
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

/** Resolve runner selectors without enumerating the matrix's Cartesian product.
 * Dynamic includes must be bound to their actual producer's rows by the caller.
 */
export function workflowExecution(file, source, matrixIncludes = new Map()) {
  const workflow = workflowMapping(file, source);
  const labels = new Set();
  /** @returns {never} */
  function fail(message) { throw new Error(file + ': ' + message); }
  const permissionScopes = new Set(['actions', 'attestations', 'checks', 'contents', 'deployments',
    'discussions', 'id-token', 'issues', 'models', 'packages', 'pages', 'pull-requests',
    'repository-projects', 'security-events', 'statuses']);
  const permissions = value => {
    if (value === undefined || value === 'read-all' || value === 'write-all') return;
    const scopes = mapping(value);
    if (!scopes) fail('invalid permissions');
    for (const [scope, access] of Object.entries(scopes)) {
      if (!permissionScopes.has(scope) || !['read', 'write', 'none'].includes(access))
        fail('unknown permission scope or access ' + scope);
    }
  };
  const shell = value => {
    if (value !== undefined && (typeof value !== 'string' || value.includes('${{')))
      fail('computes a shell dynamically; use the runner default or a literal shell');
  };
  permissions(workflow.permissions);
  shell(mapping(mapping(workflow.defaults)?.run)?.shell);
  const lookup = (value, path) => Array.isArray(value)
    ? value.flatMap(item => lookup(item, path))
    : path.length ? lookup(mapping(value)?.[path[0]], path.slice(1))
      : value === undefined ? [] : [value];
  for (const [name, value] of Object.entries(mapping(workflow.jobs) ?? {})) {
    const job = mapping(value);
    if (!job) fail('invalid job ' + name);
    permissions(job.permissions);
    shell(mapping(mapping(job.defaults)?.run)?.shell);
    if (Array.isArray(job.steps)) for (const step of job.steps) shell(mapping(step)?.shell);
    if (!Object.hasOwn(job, 'runs-on')) {
      if (Object.hasOwn(job, 'uses')) continue;
      fail('job ' + name + ' has no runner');
    }
    const matrix = mapping(mapping(job.strategy)?.matrix);
    const included = matrix?.include;
    const rows = typeof included === 'string' ? matrixIncludes.get(included) : included;
    if (included !== undefined && !Array.isArray(rows)) fail('unresolved matrix include in ' + name);
    const resolve = value => {
      if (Array.isArray(value)) {
        if (!value.length) fail('empty runner selection in ' + name);
        for (const item of value) resolve(item);
      } else if (mapping(value)) {
        const selector = /** @type {Record<string, unknown>} */ (value);
        if (Object.keys(selector).some(key => key !== 'labels')) fail('unsupported runner group in ' + name);
        resolve(selector.labels);
      } else if (typeof value === 'string' && !value.includes('${{')) {
        if (!value.trim()) fail('empty runner label in ' + name);
        labels.add(value);
      } else {
        const match = typeof value === 'string' && value.match(/^\$\{\{\s*matrix\.([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)*)\s*\}\}$/);
        if (!match) fail('unresolved runner selection in ' + name);
        const path = match[1].split('.');
        const candidates = [...lookup(matrix?.[path[0]], path.slice(1)), ...lookup(rows, path)];
        if (!candidates.length || candidates.some(candidate => typeof candidate !== 'string' || candidate.includes('${{')))
          fail('unresolved matrix runner in ' + name);
        for (const candidate of candidates) resolve(candidate);
      }
    };
    resolve(job['runs-on']);
  }
  return labels;
}
