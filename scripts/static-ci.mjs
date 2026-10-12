import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import {pinnedActions} from './static-policy.mjs';

const all=execFileSync('git',['ls-files','-z','--','.',':!managed-agents'],{encoding:'utf8'}).split('\0').filter(file=>file && fs.existsSync(file));
execFileSync('git',['diff','--check','HEAD^1','HEAD','--','.',':!managed-agents'],{stdio:'inherit'});
for(const file of all){
 if(/^\.github\/(workflows\/.*\.ya?ml|actions\/.*\/action\.ya?ml)$/.test(file))pinnedActions(file,fs.readFileSync(file,'utf8'));
 if(file.endsWith('.json'))JSON.parse(fs.readFileSync(file,'utf8'));
 if(/\.(?:mjs|cjs|js)$/.test(file))execFileSync(process.execPath,['--check',file],{stdio:'inherit'});
}
console.log('Release static checks passed');
