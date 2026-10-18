#!/usr/bin/env node
// Physical X11 clipboard + host attach through the installed GraphCoder and shared client.
import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {once} from "node:events";
import {writeFileSync} from "node:fs";
import {join,isAbsolute} from "node:path";
import {fixture} from "./graphcoder-fixture.mjs";
import {Terminal,screenText} from "./graphcoder-pty/screen.mjs";
const [binary]=process.argv.slice(2);
assert.ok(isAbsolute(binary));
const host=await fixture({images:true});
const png=Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGP4/x8AAwAB//wl3FEAAAAASUVORK5CYII=","base64");
const path=join(host.root,"selected image.png");writeFileSync(path,png);
const screen=new Terminal({cols:100,rows:30,allowProposedApi:true});
const signal=AbortSignal.timeout(8000);
let clipboard,child,clipboardClosed,childClosed,stage=0,output="",failure;
try {
    clipboard=spawn("xclip",["-selection","clipboard","-t","image/png","-i","-quiet"],{stdio:["pipe","pipe","pipe"]});
    clipboardClosed=new Promise(resolve=>{clipboard.once("close",resolve);clipboard.once("error",resolve);});
    const ready=once(clipboard.stderr,"data",{signal});
    const stopped=once(clipboard,"close",{signal}).then(([code,status])=>{throw new Error(`clipboard exited before readiness: ${status??code}`);});
    clipboard.stdin.end(png);
    await Promise.race([ready,stopped]);
    const quote=text=>`'${text.replaceAll("'", "'\\''")}'`;
    child=spawn("/usr/bin/script",["-q","-e","-f","-c",`stty rows 30 cols 100; exec ${[binary,...host.args].map(quote).join(" ")}`,"/dev/null"],{env:{...process.env,TERM:"xterm-256color"},stdio:["pipe","pipe","pipe"]});
    childClosed=new Promise(resolve=>{child.once("close",resolve);child.once("error",resolve);});
    const submit=text=>child.stdin.write(`\x1b[200~${text}\x1b[201~\r`);
    child.stdout.on("data",bytes=>{output+=bytes.toString();screen.write(bytes,()=>{
        if(failure)return;
        try {
            const visible=screenText(screen);
            if(stage===0&&visible.includes("GraphCoder")&&output.includes("\x1b[?25h")){stage=1;submit(`/attach ${path}`);}
            else if(stage===1&&/1 image/i.test(visible)){stage=2;writeFileSync(path,"host file replaced");child.stdin.write("\x16");}
            else if(stage===2&&/2 images/i.test(visible)){stage=3;submit("");}
            else if(stage===3&&visible.includes("test answer")&&visible.includes("○")){stage=4;child.stdin.write("\x04");}
        }catch(error){failure=error;child.kill();}
    });});
    child.stderr.on("data",bytes=>{output+=bytes.toString();});
    const [code,status]=await once(child,"close",{signal});
    if(failure)throw failure;
    assert.equal(status??code,0,output);assert.equal(stage,4,screenText(screen));
    assert.equal(host.imageRequests.length,1);
    const probes=[], counts=[];
    for(const request of host.imageCounts) {
        if(request.input.length===1 && request.input[0].role==="user" && request.input[0].content.length===1 && request.input[0].content[0].type==="input_text" && request.input[0].content[0].text==="token accounting") probes.push(request);
        else counts.push(request);
    }
    assert.equal(probes.length,1,"one fixed-tool accounting probe precedes user input");
    assert.ok(counts.length>=1,"the user image input must receive verified counting");
    for(const request of [...counts,...host.imageRequests]) {
        const images=request.input.flatMap(message=>Array.isArray(message.content)?message.content:[]).filter(part=>part.type==="input_image");
        assert.equal(images.length,2,JSON.stringify(request));
        assert.equal(images[0].image_url,`data:image/png;base64,${png.toString("base64")}`);
        assert.match(images[1].image_url,/^data:image\/jpeg;base64,/);
        const jpeg=Buffer.from(images[1].image_url.split(",")[1],"base64");
        assert.equal(jpeg[0],255);assert.equal(jpeg[1],216);
    }
    console.log(`image PTY: attach + physical clipboard paste + pinned private refs + provider parts passed`);
}catch(error){
    throw signal.aborted ? new Error(`image PTY timed out at ${stage}: ${screenText(screen)}`,{cause:error}) : error;
}finally{
    child?.kill();clipboard?.kill();
    await Promise.all([childClosed,clipboardClosed]);
    screen.dispose();await host.close();
}
