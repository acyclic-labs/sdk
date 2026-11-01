import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

// Each gate is an original child pipe, armed by spawn before the shell runs.
// Only a producer record releases it; shell exit closes every unfinished gate.
export function runShellFixture(bash, script, { gates, env, recovery, event }) {
  return new Promise((resolve, reject) => {
    const child = spawn(bash, ["-c", script], {
      env, stdio: ["ignore", "pipe", "pipe", ...Array.from({ length: gates }, () => "pipe")],
    });
    const inputs = child.stdio.slice(3);
    let stdout = "", stderr = "", failure, exited = false;
    const fail = error => {
      failure ??= new Error(`step=join original shell fixture: ${error.message}; recovery command: ${recovery}`, { cause: error });
      for (const input of inputs) input.end();
      child.kill();
    };
    const release = (index, value) => {
      const input = inputs[index];
      if (!input || input.writableEnded) throw new Error(`gate ${index} has no remaining producer`);
      input.end(value + "\n");
    };
    for (const input of inputs) input.on("error", error => {
      if (!exited || error.code !== "EPIPE") fail(error);
    });
    child.stdout.on("data", chunk => { stdout += chunk.toString(); });
    child.stderr.on("data", chunk => { stderr += chunk.toString(); });
    child.stdout.on("error", fail);
    child.stderr.on("error", fail);
    createInterface({ input: child.stdout }).on("line", line => {
      if (exited) return;
      try { event(line, release); }
      catch (error) { fail(error); }
    }).on("error", fail);
    child.on("error", fail);
    child.on("exit", status => {
      exited = true;
      if (status === 0 && inputs.some(input => !input.writableEnded))
        failure ??= new Error(`step=join original shell fixture: producer exited before its gate event; recovery command: ${recovery}`);
      for (const input of inputs) input.end();
    });
    child.on("close", (status, signal) => {
      if (failure) reject(failure);
      else resolve({ status, signal, stdout, stderr });
    });
  });
}
