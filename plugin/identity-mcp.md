# Identity MCP

Configure a stdio MCP server with the installed native CLI:

```json
{
  "mcpServers": {
    "acyclic-identity": {
      "command": "acyclic",
      "args": ["identity-mcp", "--endpoint", "https://YOUR_PUBLIC_EDGE_ORIGIN"]
    }
  }
}
```

Set `ACYCLIC_IDENTITY_FILE` or add `--credentials PATH` to choose a credential
file. The default is `~/.acyclic/identity.json`, shared with the identity CLI.
The server supports MCP protocol version `2025-11-25`; stdout contains only
newline-delimited JSON-RPC messages, as required by the
[stdio specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports).

All tools take an empty object:

| Tool | Operation |
| --- | --- |
| `identity_login` | Start device approval and return the URL and user code immediately. |
| `identity_login_complete` | Wait for approval of the pending login, then save the session. |
| `identity_whoami` | Read the current identity, refreshing expired access automatically. |
| `identity_orgs` | Read visible organizations, refreshing expired access automatically. |
| `identity_refresh` | Rotate the stored refresh token. |

Relay the approval URL and code to the user between the two login calls.
Device codes remain inside the MCP process. Pending approvals are scoped to
that process and expire from the time they were issued; restarting the server
requires a new approval flow. Completion keeps the approval after retryable transport or server failures. Pings remain responsive during polling. Cancellation stops further polls and retains
the approval for retry. A dispatched exchange finishes and saves successful
credentials before releasing the operation, even after cancellation or stdin EOF. Credential locking and rotation use the same
implementation as the CLI.

Successful tools return text JSON plus `structuredContent`. API failures are
tool results with `isError: true`; unknown tools and invalid arguments are
JSON-RPC invalid-parameter errors. Tokens never appear in tool results.
No workspace service is started by this server.
