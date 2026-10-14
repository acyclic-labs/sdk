import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import {pinnedActions} from './static-policy.mjs';

const all=execFileSync('git',['ls-files','-z','--','.',':!managed-agents'],{encoding:'utf8'}).split('\0').filter(file=>file && fs.existsSync(file));
const git=(...args)=>execFileSync('git',args,{encoding:'utf8'}).trim();
if(git('rev-parse','--is-shallow-repository')==='true')throw new Error('Release whitespace checks require complete history and release tags');
// Exclude HEAD so a retry after publication still checks the same release range.
const tags=git('tag','--merged','HEAD','--no-contains','HEAD','--list','acyclic-v[0-9]*');
const base=tags ? git('describe','--tags','--match','acyclic-v[0-9]*','--abbrev=0','HEAD^1')
 : execFileSync('git',['hash-object','-w','-t','tree','--stdin'],{input:'',encoding:'utf8'}).trim();
console.log('Checking release whitespace from '+base+' to HEAD');
execFileSync('git',['diff','--check',base,'HEAD','--','.',':!managed-agents'],{stdio:'inherit'});
for(const file of all){
 if(/^\.github\/(workflows\/.*\.ya?ml|actions\/.*\/action\.ya?ml)$/.test(file))pinnedActions(file,fs.readFileSync(file,'utf8'));
 if(file.endsWith('.json'))JSON.parse(fs.readFileSync(file,'utf8'));
 if(/\.(?:mjs|cjs|js)$/.test(file))execFileSync(process.execPath,['--check',file],{stdio:'inherit'});
}
console.log('Release static checks passed');
