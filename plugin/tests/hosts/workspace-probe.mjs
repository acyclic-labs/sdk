import {mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {start,Spool} from './native-fixture.mjs';

// Exercise the product's sole capability probe with a real, empty device root.
// The owner uses the same open/dispatch implementation as graphcoder.
export async function probeOwner(binary,workspace,state,root,env){
 const probe=join(root,'native-host');mkdirSync(join(probe,'dev'),{recursive:true});
 const owner=start(binary,[workspace,state,probe],{cwd:workspace,env});owner.collect();
 const events=new Spool(owner.child.stdout,owner.closed,
  {key:event=>event?.type==='workspace-owner-ready'?['ready']:[],unique:true});
 try{await events.keyed('ready');}
 catch(error){owner.child.stdin.end();const result=await owner.closed;throw Object.assign(new Error(result.stderr,{cause:error}),{result});}
 return {async close(){owner.child.stdin.end();return await owner.closed;}};
}
