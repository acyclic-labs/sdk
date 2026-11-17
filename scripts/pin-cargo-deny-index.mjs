// Pin the exact registry entries needed by Cargo.lock. The offline security
// check reads these bytes, including yanked flags, as declared Bazel inputs.
import {createHash} from 'node:crypto';
import {toml} from './policy-core.mjs';

const commit='1cd7d9502156b501a3ac818b86aa3a277b7d8071';
const root=process.env.BUILD_WORKSPACE_DIRECTORY??process.cwd();
const lock=toml(root,'Cargo.lock');
const names=[...new Set(lock.package.filter(pkg=>pkg.source?.startsWith('registry+')).map(pkg=>{
 if(pkg.source!=='registry+https://github.com/rust-lang/crates.io-index')throw Error('unhandled registry '+pkg.source);
 return pkg.name;
}))].sort();
const indexPath=value=>{const name=value.toLowerCase();return name.length===1?'1/'+name:name.length===2?'2/'+name:name.length===3?'3/'+name[0]+'/'+name:name.slice(0,2)+'/'+name.slice(2,4)+'/'+name;};
const result={};let next=0;
await Promise.all(Array.from({length:16},async()=>{
 for(;;){
  const i=next++;if(i>=names.length)return;
  const name=names[i],url=`https://raw.githubusercontent.com/rust-lang/crates.io-index/${commit}/${indexPath(name)}`;
  const response=await fetch(url);if(!response.ok)throw Error(`${name}: index fetch returned ${response.status}`);
  result[name]=createHash('sha256').update(Buffer.from(await response.arrayBuffer())).digest('hex');
 }
}));
console.log('DENY_INDEX_LOCK='+JSON.stringify({commit,packages:Object.fromEntries(names.map(name=>[name,result[name]]))}));
