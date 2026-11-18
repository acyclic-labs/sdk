import {repositoryRoot, testFeatureFindings} from './policy-core.mjs';

if (import.meta.main) {
  const errors = testFeatureFindings(repositoryRoot);
  if (errors.length) {
    console.error(`STEP_FAILED test-feature-hygiene\n${errors.join('\n')}\nrecovery: correct the listed test manifests (loom/kani require model-checking), then run node scripts/check-test-features.mjs`);
    process.exitCode = 1;
  } else console.log('Test feature hygiene: every feature/model-gated Cargo target opts in explicitly.');
}
