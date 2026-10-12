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
| `identity_signup` | Generate an Ed25519 key and solve hashcash for an agent registration; requires new or pending agent credentials. |
| `identity_whoami` | Read the current identity, refreshing expired access automatically. |
| `identity_orgs` | Read visible organizations, refreshing expired access automatically. |
| `identity_refresh` | Rotate the session refresh token or renew an agent-only certificate with its key. |

Relay the approval URL and code to the user between the two login calls.
Device codes remain inside the MCP process. Pending approvals are scoped to
that process and expire from the time they were issued; restarting the server
requires a new approval flow. Completion keeps the approval after retryable transport or server failures. Pings remain responsive during polling; notifications/cancelled stops that request and retains the approval for retry. Credential locking and rotation use the same
implementation as the CLI.

Agent registration returns the created user and org identifiers and public
certificate. Identity and org reads require OAuth device login, which upgrades
an existing agent with a signed request. Signup preserves a pending private
key if the server outcome is ambiguous; see [the API contract](identity-api.md).

Successful tools return text JSON plus `structuredContent`. API failures are
tool results with `isError: true`; unknown tools and invalid arguments are
JSON-RPC invalid-parameter errors. Tokens never appear in tool results.
No workspace service is started by this server.
