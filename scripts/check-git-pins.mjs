import {checkFetchableGitPins} from './policy-core.mjs';

console.log(`Git pins: ${checkFetchableGitPins(process.cwd())} remote/commit pairs are fetchable`);
