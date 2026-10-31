---
version: 1
tools:
  - {capability: shell, variant: shell, revision: openai-shell-1}
  - {capability: fs, variant: fs, revision: '2'}
  - {capability: edit, variant: apply_patch, revision: openai-v4a-1}
  - {capability: spawn, variant: spawn, revision: agent-1}
  - {capability: send, variant: send, revision: agent-1}
  - {capability: wait, variant: wait, revision: agent-1}
  - {capability: archive, variant: archive, revision: agent-1}
  - {capability: web_search, variant: web_search, revision: native-web-search-1}
  - {capability: call, variant: call, revision: skill-call-1}
reasoning: {mode: effort, effort: medium}
cache: automatic
---
