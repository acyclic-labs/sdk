---
version: 1
tools:
  - {capability: shell, variant: shell, revision: '2'}
  - {capability: fs, variant: fs, revision: '2'}
  - {capability: read, variant: read, revision: portable-7}
  - {capability: write, variant: write_file, revision: trained-file-1}
  - {capability: edit, variant: replace, revision: trained-file-1}
  - {capability: spawn, variant: spawn, revision: agent-1}
  - {capability: send, variant: send, revision: agent-1}
  - {capability: wait, variant: wait, revision: agent-1}
  - {capability: archive, variant: archive, revision: agent-1}
  - {capability: web_search, variant: web_search, revision: native-web-search-1}
  - {capability: call, variant: call, revision: skill-call-1}
reasoning: {mode: level, effort: medium}
cache: automatic
---
