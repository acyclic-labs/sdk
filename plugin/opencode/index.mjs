// Acyclic native workspace shim. OpenCode owns its tools, agents and loop.
import { workspace, nativeSession } from "../shared/workspace.mjs";

const executable = "__ACYCLIC_EXECUTABLE__";

export default async function acyclic({client, directory}) {
  let selected;
  let session;
  function select(id) {
    if (id !== session || !selected) {
      session = id;
      // Cache the admission itself. Concurrent hooks retain their own identity
      // even when another native session becomes the most recently selected.
      const admission = (async () => {
        const original = await client.session.get({path: {id}});
        if (original.error || !original.data)
          throw new Error("step=admit OpenCode session: native session absent; recovery=opencode");
        // Native children keep their host's original cwd and lifecycle.
        if (original.data.parentID) return;
        return nativeSession(executable, "opencode", directory, id);
      })().catch(error => {
        // A failed attempt owns no admission. Its completion must not evict
        // another session selected while this attempt was still in flight.
        if (selected === admission) selected = undefined;
        throw error;
      });
      selected = admission;
    }
    return selected;
  }
  return {
    config: async config => {
      // Isolated delegation belongs to the shared actor/subtree/volume owner.
      // Deny the native Task tool even when a project grants other tools.
      if (typeof config.permission !== "object" || config.permission === null)
        config.permission = { "*": config.permission ?? "ask" };
      config.permission.task = "deny";
      config.mcp ??= {};
      if (Object.hasOwn(config.mcp, "acyclic-sessions"))
        throw new Error("step=install OpenCode coordinator: acyclic-sessions already configured; recovery=remove the duplicate native MCP registration");
      config.mcp["acyclic-sessions"] = {
        type: "local", command: [executable, "__mcp", "opencode"], enabled: true,
      };
    },
    "chat.message": async input => { await select(input.sessionID); },
    "tool.execute.before": async input => {
      // Commands can bypass OpenCode's Task permission check. Fence the effect
      // at the native pre-tool boundary as well as removing it from the menu.
      if (input.tool === "task")
        throw new Error("step=admit native delegation: use acyclic-sessions_spawn; recovery=call acyclic-sessions_spawn");
    },
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
