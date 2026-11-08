import assert from 'node:assert/strict';
import {test} from 'node:test';
import {scriptedScenario} from './qualify-scripted.mjs';

test('framing admits only its CLI artifact without Codex or GraphCoder',()=>{
 const artifacts={artifacts:{acyclic:{path:process.execPath},
  get graphcoder(){throw new Error('unselected GraphCoder dependency');}}};
 const scenario=scriptedScenario(artifacts,undefined,'managed:bounded-framing');
 assert.equal(scenario.selection,'managed:bounded-framing');
 assert.equal(scenario.args.length,2);
 assert.equal(scenario.args[1],process.execPath);
 assert.ok(scenario.args[0].endsWith('mcp-framing.py'));
});

test('scale does not require Codex; managed sandbox scenarios refuse its absence',()=>{
 const artifacts={artifacts:{acyclic:{path:process.execPath},graphcoder:{path:process.execPath}}};
 for(const host of ['codex','claude-code','pi'])
  assert.equal(scriptedScenario(artifacts,undefined,'scale:'+host).args.at(-1),host);
 for(const name of ['lifecycle','recursive','resume'])
  assert.throws(()=>scriptedScenario(artifacts,undefined,'managed:'+name),
   /step=admit managed sandbox executable; recovery:/);
});
