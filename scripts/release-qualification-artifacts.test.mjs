import assert from 'node:assert/strict';
import test from 'node:test';
import {selectArtifacts} from './release-qualification-artifacts.mjs';

test('interrupted qualification retains successful lanes from this run only', () => {
  const list = ['packages-linux-12-1','packages-macos-12-2','stream-native-windows-12-1',
    'packages-linux-13-3','packages-linux-12-3'].map(name => ({name,expired:false}));
  assert.deepEqual(selectArtifacts(list,'12',2), {
    'packages-linux':'packages-linux-12-1','packages-macos':'packages-macos-12-2',
    'stream-native-windows':'stream-native-windows-12-1',
  });
});
test('missing or expired qualification artifacts cannot be published', () => {
  const list = ['packages-linux-12-1','packages-macos-12-1','stream-native-windows-12-1']
    .map(name => ({name,expired:name.startsWith('packages-macos')}));
  assert.throws(() => selectArtifacts(list,'12',2), /Missing qualified packages-macos/);
  assert.throws(() => selectArtifacts(list,'different',2), /exact release run/);
});
