import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
const git=(...args)=>execFileSync('git',args,{encoding:'utf8'}).trim();
const all=git('ls-files','-z','--','.',':!managed-agents').split('\0').filter(Boolean);
const event=process.env.GITHUB_EVENT_PATH?JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH,'utf8')):{};
let base=process.env.GITHUB_EVENT_NAME==='push'?event.before:'HEAD^1';
let changed;
try{
 if(!base||/^0+$/.test(base))throw new Error('No base');
 git('rev-parse','--verify',base+'^{commit}');
 changed=git('diff','--no-renames','--name-only','-z',base,'HEAD','--','.',':!managed-agents').split('\0').filter(Boolean);
}catch(error){
 // A shallow multi-commit push must check everything rather than silently miss files.
 console.log('Diff base unavailable; checking all tracked files');
 base=null;changed=all;
}
execFileSync('git',['diff','--check',...(base?[base,'HEAD']:['--cached']),'--','.',':!managed-agents'],{stdio:'inherit'});
for(const file of all.filter(p=>/^\.github\/(workflows\/.*\.ya?ml|actions\/.*\/action\.ya?ml)$/.test(p))){
 for(const line of fs.readFileSync(file,'utf8').split('\n')){
  const use=line.match(/^\s*(?:-\s*)?uses:\s*["']?([^\s"'#]+)/);
  if(use&&!use[1].startsWith('./')&&!/^[^@]+@[0-9a-f]{40}$/.test(use[1]))throw new Error(file+': action must be pinned: '+use[1]);
 }
}
for(const file of changed.filter(p=>fs.existsSync(p))){
 if(file.endsWith('.json'))JSON.parse(fs.readFileSync(file,'utf8'));
 if(/\.(?:mjs|cjs|js)$/.test(file))execFileSync(process.execPath,['--check',file],{stdio:'inherit'});
}
if(changed.some(p=>p.endsWith('.rs')||p==='rust-toolchain.toml')){
 const channel=fs.readFileSync('rust-toolchain.toml','utf8').match(/^channel\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"$/m)?.[1];
 if(!channel)throw new Error('An exact Rust toolchain version is required');
 execFileSync('rustup',['toolchain','install',channel,'--no-self-update','--profile','minimal','--component','rustfmt'],{stdio:'inherit'});
 execFileSync('rustup',['run',channel,'cargo','fmt','--all','--','--check'],{stdio:'inherit'});
}
if(process.env.GITHUB_OUTPUT)fs.appendFileSync(process.env.GITHUB_OUTPUT,'iac='+changed.some(p=>p.startsWith('iac/')||p==='.opentofu-version')+'\n');
console.log('Static checks passed');

execFileSync(process.execPath,['scripts/check-workflow-runners.mjs'],{stdio:'inherit'});
execFileSync(process.execPath,['scripts/check-compatibility-digests.mjs'],{stdio:'inherit'});
