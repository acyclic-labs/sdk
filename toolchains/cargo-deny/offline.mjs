import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';

const root=process.env.TEST_SRCDIR,workspace=process.env.TEST_WORKSPACE,tmp=process.env.TEST_TMPDIR;
if(!root||!workspace||!tmp)throw Error('Bazel test runfiles are required');
const input=path.join(root,workspace),source=path.join(input,'deny-workspace'),tree=path.join(tmp,'workspace');
fs.cpSync(source,tree,{recursive:true,dereference:true});
const home=path.join(tmp,'cargo-home'),db=path.join(home,'advisory-dbs','advisory-db-3157b0e258782691');
fs.mkdirSync(home,{recursive:true});fs.cpSync(path.join(input,'deny-advisories'),db,{recursive:true,dereference:true});
const advisories=fs.readdirSync(path.join(db,'crates'),{recursive:true,withFileTypes:true}).filter(entry=>entry.isFile()&&entry.name.endsWith('.md'));
if(!advisories.length)throw Error('pinned advisory snapshot contains no regular advisory files');
// The committed snapshot's actual fetch time, not the time this test happened to run.
fs.mkdirSync(path.join(db,'.git'));const fetchHead=path.join(db,'.git','FETCH_HEAD');fs.writeFileSync(fetchHead,'');
const pinned=new Date('2026-11-17T10:14:35Z');fs.utimesSync(fetchHead,pinned,pinned);
const registry=path.join(input,'deny-registry');
fs.writeFileSync(path.join(home,'config.toml'),`[source.crates-io]\nreplace-with = "deny-registry"\n[source.deny-registry]\nlocal-registry = ${JSON.stringify(registry)}\n[net]\noffline = true\n`);
const compiler=path.join(input,'deny-compiler'),cargo=path.join(compiler,'bin','cargo'),rustc=path.join(compiler,'bin','rustc');
const binary=path.join(input,'deny-tool','cargo-deny');
const env={HOME:tmp,CARGO_HOME:home,CARGO:cargo,RUSTC:rustc,PATH:path.join(compiler,'bin'),LD_LIBRARY_PATH:path.join(compiler,'lib'),CARGO_NET_OFFLINE:'true'};
const child=spawnSync(binary,['--manifest-path',path.join(tree,'Cargo.toml'),'--frozen','--format','json','check'],{cwd:tree,env,encoding:'utf8',maxBuffer:64*1024*1024});
if(child.error)throw child.error;
process.stdout.write(child.stdout);process.stderr.write(child.stderr);
process.exitCode=child.status??1;
