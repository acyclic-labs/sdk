import assert from 'node:assert/strict';
import {test} from 'node:test';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {join} from 'node:path';
import {fixtureEnvironment} from './native-fixture.mjs';
import {piCommand} from './sandbox-commands.mjs';

test('actual pinned Pi RPC waits for command completion after streamed output',async()=>{
 const native=realpathSync(process.env.ACYCLIC_NATIVE_PI);
 assert.ok(process.env.E2E_SCRATCH_ROOT,'step=admit Pi RPC scratch; recovery: set E2E_SCRATCH_ROOT');
 const root=mkdtempSync(join(process.env.E2E_SCRATCH_ROOT,'pi-rpc-'));
 try{await piCommand(native,'printf NATIVE_PI_RPC_COMPLETED',{cwd:root,env:fixtureEnvironment(join(root,'profile'))});}
 finally{rmSync(root,{recursive:true,force:true});}
});
