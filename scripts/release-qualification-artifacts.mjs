import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

// Failed-job retries can retain successful lanes from an earlier attempt of
// this same release run. Freeze their explicit names after all lanes pass.
export function selectArtifacts(artifacts, run, attempt) {
  if (!/^[1-9][0-9]*$/.test(String(run)) || !Number.isSafeInteger(Number(attempt)) || Number(attempt) < 1) {
    throw new Error('An exact release run and attempt are required');
  }
  const selected = {};
  for (const lane of ['packages-linux', 'packages-macos', 'stream-native-windows']) {
    const matches = artifacts.filter(artifact => {
      const prefix = `${lane}-${run}-`;
      const suffix = artifact.name?.startsWith(prefix) ? artifact.name.slice(prefix.length) : '';
      return !artifact.expired && /^[1-9][0-9]*$/.test(suffix) && Number(suffix) <= Number(attempt);
    }).sort((a, b) => Number(b.name.split('-').at(-1)) - Number(a.name.split('-').at(-1)));
    if (!matches.length) throw new Error(`Missing qualified ${lane} in release run ${run}`);
    if (matches.length > 1 && matches[0].name === matches[1].name) throw new Error(`Ambiguous qualified ${lane}`);
    selected[lane] = matches[0].name;
  }
  return selected;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const {GITHUB_REPOSITORY: repo, GITHUB_RUN_ID: run, GITHUB_RUN_ATTEMPT: attempt, GITHUB_OUTPUT: output} = process.env;
  const pages = JSON.parse(execFileSync('gh', ['api', '--paginate', '--slurp',
    `repos/${repo}/actions/runs/${run}/artifacts?per_page=100`], {encoding:'utf8'}));
  const artifacts = selectArtifacts(pages.flatMap(page => page.artifacts), run, attempt);
  fs.appendFileSync(output, `artifacts=${JSON.stringify(artifacts)}\n`);
}
