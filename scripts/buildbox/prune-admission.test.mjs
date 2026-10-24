import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

test('failed shared maintenance cannot refuse either independently admitted worktree', async () => {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'prune-admission-'));
  const original=process.stderr.write, diagnostics=[];
  try {
    const source=fs.readFileSync(new URL('./buildbox.mjs',import.meta.url),'utf8');
    assert.ok(source.includes('await maintainSourceCopies(co.repo, notes);'));
    fs.writeFileSync(path.join(root,'generator.mjs'),source+'\nexport {maintainSourceCopies};\n');
    fs.copyFileSync(new URL('./buildbox-receipt.mjs',import.meta.url),path.join(root,'buildbox-receipt.mjs'));
    const {maintainSourceCopies}=await import(path.join(root,'generator.mjs'));
    process.stderr.write=(text)=>{diagnostics.push(String(text));return true;};
    const prepared=[];
    const result=await Promise.all(['bad','good'].map(async name=>{
      const notes=[];
      await maintainSourceCopies('sdk',notes,async()=>({status:name==='bad'?224:0,stderr:Buffer.from('foreign-owned tomb')}));
      prepared.push(name);return notes;
    }));
    assert.deepEqual(prepared.sort(),['bad','good']);
    assert.match(result[0][0],/BB_WT_PRUNE_FAILED rc=224: foreign-owned tomb/);
    assert.deepEqual(result[1],[]);
    assert.match(diagnostics.join(''),/BB_WT_PRUNE_FAILED/);
    await maintainSourceCopies('sdk',[],async()=>{throw new Error('maintenance transport interrupted');});
    assert.match(diagnostics.join(''),/maintenance transport interrupted/);
    await maintainSourceCopies('cloud',[],async()=>{throw new Error('must not prune another repo');});
    // The former fatal maintenance branch rejects even a healthy admission.
    async function oldAdmission(){
      const prune={status:224,stderr:Buffer.from('another worktree is bad')};
      if(prune.status!==0)throw new Error('worktree age/count admission refused');
      prepared.push('old-good');
    }
    await assert.rejects(oldAdmission,/worktree age\/count admission refused/);
    assert.ok(!prepared.includes('old-good'));
  } finally {
    process.stderr.write=original;
    fs.rmSync(root,{recursive:true,force:true});
  }
});
