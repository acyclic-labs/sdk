// Acyclic native workspace shim. OpenCode owns its tools, agents and loop.
import { workspace, nativeSession } from "../shared/workspace.mjs";

const executable = "__ACYCLIC_EXECUTABLE__";

export default async function acyclic({client, directory}) {
  let selected;
  let session;
  function select(id) {
    if (id !== session) {
      session = id;
      // Cache the admission itself. Concurrent hooks retain their own identity
      // even when another native session becomes the most recently selected.
      selected = (async () => {
        const original = await client.session.get({path: {id}});
        if (original.error || !original.data)
          throw new Error("step=admit OpenCode session: native session absent; recovery=opencode");
        // Native children keep their host's original cwd and lifecycle.
        if (original.data.parentID) return;
        return nativeSession(executable, "opencode", directory, id);
      })();
    }
    return selected;
  }
  return {
    config: async config => {
      config.mcp ??= {};
      if (Object.hasOwn(config.mcp, "acyclic-sessions"))
        throw new Error("step=install OpenCode coordinator: acyclic-sessions already configured; recovery=remove the duplicate native MCP registration");
      config.mcp["acyclic-sessions"] = {
        type: "local", command: [executable, "__mcp", "opencode"], enabled: true,
      };
    },
    "chat.message": async input => { await select(input.sessionID); },
    "tool.execute.after": async input => {
      const location = await select(input.sessionID);
      if (location) await workspace(executable, directory, {
        kind: "hook", host: "opencode", event: "ToolBoundary", input: {task: location.task},
      });
    },
    event: async ({event}) => {
      if (event.type === "session.idle") {
        const location = await select(event.properties.sessionID);
        if (location) await workspace(executable, directory, {kind: "hook", host: "opencode",
          event: "Stop", input: {task: location.task}});
      }
    },
  };
}
