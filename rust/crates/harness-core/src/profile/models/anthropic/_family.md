---
version: 1
tools:
  - {capability: shell, variant: shell, revision: '2'}
  - {capability: fs, variant: fs, revision: '2'}
  - {capability: edit, variant: str_replace_based_edit_tool, revision: text_editor_20250728}
  - {capability: spawn, variant: spawn, revision: agent-1}
  - {capability: send, variant: send, revision: agent-1}
  - {capability: wait, variant: wait, revision: agent-1}
  - {capability: archive, variant: archive, revision: agent-1}
  - {capability: web_search, variant: web_search, revision: native-web-search-1}
reasoning: {mode: adaptive, effort: medium}
cache: ephemeral
---
