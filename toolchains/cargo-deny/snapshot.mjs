// Cargo metadata needs target paths, but not Rust source bytes. Materializing
// stable stubs makes the policy test's inputs independent of source edits.
import fs from 'node:fs';
import path from 'node:path';

const [output,mapFile]=process.argv.slice(2);
for(const [source,relative] of JSON.parse(fs.readFileSync(mapFile,'utf8'))){
 if(path.isAbsolute(relative)||relative.split('/').includes('..'))throw Error('invalid workspace input '+relative);
 const destination=path.join(output,relative);fs.mkdirSync(path.dirname(destination),{recursive:true});
 if(relative.endsWith('.rs'))fs.writeFileSync(destination,'');
 else fs.copyFileSync(source,destination);
}
