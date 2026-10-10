import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
export function checkReleaseVersion(version){
 if(!/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$/.test(version||''))throw new Error('A committed semver release version is required');
 return version;
}
export function checkReleaseSource(sha,candidate){
 if(candidate&&sha!==candidate)throw new Error('Release source differs from the dispatch candidate');
}
export function checkNpmVersions(version,packages,read=file=>JSON.parse(fs.readFileSync(file,'utf8'))){
 for(const item of packages){
  const file=item.source==='typescript'?'typescript/packages/'+item.directory+'/package.json':'plugin/package.json';
  const manifest=read(file);
  if(manifest.name!==item.name||manifest.version!==version||manifest.private!==false)throw new Error(file+' does not match coordinated release '+version);
 }
}
export function releaseSource(){
const version=checkReleaseVersion(process.env.RELEASE_VERSION);
const sha=execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim();
checkReleaseSource(sha,process.env.GITHUB_SHA);
checkNpmVersions(version,JSON.parse(fs.readFileSync('release/npm-packages.json','utf8')));
execFileSync(process.execPath,['scripts/publish-cargo-crates.mjs','check',sha,version],{stdio:'inherit'});
fs.appendFileSync(process.env.GITHUB_OUTPUT,`sha=${sha}\nversion=${version}\nrun=${process.env.GITHUB_RUN_ID}\nattempt=${process.env.GITHUB_RUN_ATTEMPT}\n`);
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))releaseSource();
