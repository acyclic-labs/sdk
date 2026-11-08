import { execFile } from "node:child_process";

// Native extensions share the installed command's workspace boundary. Native
// provider credentials, transcripts and tools never enter this request.
export function workspace(executable, cwd, action) {
  return new Promise((yes, no) => {
    let inputFailure;
    const child = execFile(executable, ["__workspace"],
      { cwd, windowsHide: true, maxBuffer: 65536 }, (error, stdout) => {
        if (error || inputFailure) { no(error ?? inputFailure); return; }
        try { yes(JSON.parse(stdout)); } catch (failure) { no(failure); }
      });
    // Retain an early native refusal until execFile observes physical close.
    child.stdin?.on("error", error => { inputFailure ??= error; });
    child.stdin?.end(JSON.stringify(action));
  });
}

export async function nativeSession(executable, host, cwd, session, file = null, onSelected) {
  const location = process.env.ACYCLIC_TASK
    ? {task: process.env.ACYCLIC_TASK, cwd}
    : await workspace(executable, cwd, {kind: "open", host, session});
  onSelected?.(location);
  await workspace(executable, cwd, {kind: "hook", host, event: "NativeSession",
    input: {task: location.task, native_session: {id: session, file}}});
  return location;
}
