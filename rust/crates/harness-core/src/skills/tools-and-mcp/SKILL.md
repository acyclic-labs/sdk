---
name: tools-and-mcp
description: Find and use the tools available to you, including tools from MCP servers.
---

Your tools are declared in the request: the file tools (`read`, `write`, `edit`), `shell`, `fs`, `code`, the agent tools (`spawn`, `send`, `wait`, `archive`) and any tools from configured MCP servers, which appear as ordinary tools. `code` runs JavaScript that calls any of them through `tools[name](arguments)`; use it for loops, filters and batches.

A tool call ends exactly once: ok, error, cancelled or indeterminate. An error reads `tool execution failed: <reason>`; adapt or report it. When the tool set changes during a run, a `tools.changed` notice is appended instead of earlier context being edited.
