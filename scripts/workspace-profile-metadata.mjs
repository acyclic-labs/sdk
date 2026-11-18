// Serialize the shared workspace model for consumers that cannot invoke Node.
import {workspaceProfileMetadata} from './workspace-model.mjs';
import path from 'node:path';

const [sourceRoot, selection, workspace = '.', config = '-', lock = '-', universe = '-', ...additionalInputs] = process.argv.slice(2);
if (!sourceRoot || !['workspace', 'all'].includes(selection)) {
  throw new Error('expected a source root and workspace|all selection');
}
process.stdout.write(JSON.stringify(workspaceProfileMetadata(path.resolve(sourceRoot, workspace), {
  sourceRoot, workspaceOnly: selection === 'workspace', additionalInputs,
  patchConfig: config === '-' ? undefined : config, lockFile: lock === '-' ? undefined : lock,
  universeFile: universe === '-' ? undefined : universe,
})));
