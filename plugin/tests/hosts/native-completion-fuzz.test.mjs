import {test} from 'node:test';
import {check,fuzz,paths,replace,seeds} from './native-completion.fuzz.mjs';

for(const record of seeds)test(record.host+' native decoder contains generated shape failures ('+(record.mode??'tools')+')',()=> {
 const lines=record.stdout.trim().split('\n');
 for(let index=0;index<lines.length;index++) {
  const original=JSON.parse(lines[index]);
  for(const path of paths(original))for(const value of [null,false,0,'',[],{},[null]]) {
   const changed=lines.slice();
   changed[index]=JSON.stringify(replace(structuredClone(original),path,value));
   check(record,changed.join('\n'));
  }
 }
});

test('byte fuzz entry retains the original decoder and raw JSON refusal',()=> {
 for(let seed=0;seed<seeds.length;seed++)for(const value of ['null','[]','{}','false','0','""','[null]','{'])
  fuzz(Buffer.concat([Buffer.from([seed,2,0]),Buffer.from(value)]));
});
