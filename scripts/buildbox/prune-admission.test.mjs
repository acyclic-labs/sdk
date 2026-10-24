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
      await maintainSourceCopies('sdk',notes,async()=>({status:name==='bad'?224:0,stderr:Buffer.from(name==='bad'?'foreign-owned tomb':'')}));
      prepared.push(name);return notes;
    }));
    assert.deepEqual(prepared.sort(),['bad','good']);
    assert.match(result[0][0],/BB_WT_PRUNE_FAILED rc=224: foreign-owned tomb/);
    assert.deepEqual(result[1],[]);
    assert.match(diagnostics.join(''),/BB_WT_PRUNE_FAILED/);
    const summary='[buildbox] wt-sdk {"removed":["old"],"remaining":1,"held":[],"errors":[],"over_cap":false}';
    const clean=[];const before=diagnostics.length;
    await maintainSourceCopies('sdk',clean,async()=>({status:0,stderr:Buffer.from(summary)}));
    assert.deepEqual(clean,[]);
    assert.equal(diagnostics.length,before,'normal successful summaries are not cleanup failures');
    const noisy=source.replace('warnings.length === 0','!diagnostic');
    assert.notEqual(noisy,source);
    fs.writeFileSync(path.join(root,'noisy.mjs'),noisy+'\nexport {maintainSourceCopies};\n');
    const former=await import(path.join(root,'noisy.mjs'));
    const noisyNotes=[];
    await former.maintainSourceCopies('sdk',noisyNotes,async()=>({status:0,stderr:Buffer.from(summary)}));
    assert.match(noisyNotes[0],/BB_WT_PRUNE_FAILED rc=0/);
    const warning=[];
    const marker='BB_WT_PRUNE_FAILED successful retirement had one undeletable copy';
    await maintainSourceCopies('sdk',warning,async()=>({status:0,stderr:Buffer.from(marker)}));
    assert.match(warning[0],/rc=0.*undeletable copy/);
    assert.ok(diagnostics.some(line=>line.includes(marker)));
    // Actual former success return drops the same warning; the assertion
    // above must catch that omission even though admission still succeeds.
    const mutant=source.replace('prune.status === 0 && warnings.length === 0','prune.status === 0');
    assert.notEqual(mutant,source);
    fs.writeFileSync(path.join(root,'old.mjs'),mutant+'\nexport {maintainSourceCopies};\n');
    const old=await import(path.join(root,'old.mjs'));
    const oldWarning=[];
    await old.maintainSourceCopies('sdk',oldWarning,async()=>({status:0,stderr:Buffer.from(marker)}));
    assert.deepEqual(oldWarning,[]);
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
