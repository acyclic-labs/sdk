---
version: 1
tools:
  - capability: read
    variant: Read
    revision: claude-agent-sdk-0.3.316-v1
    arguments:
      path: $file_path
      selection: {kind: lines, offset: {$one_based: offset}, limit: $limit}
    refuse: [pages]
  - capability: write
    variant: Write
    revision: claude-agent-sdk-0.3.316-v1
    arguments: {path: $file_path, content: $content}
  - capability: edit
    variant: Edit
    revision: claude-agent-sdk-0.3.316-v1
    arguments: {path: $file_path, old_text: $old_string, new_text: $new_string, replace_all: $replace_all}
  - {capability: glob, variant: Glob, revision: claude-agent-sdk-0.3.316-v1}
  - {capability: search, variant: Grep, revision: claude-agent-sdk-0.3.316-v1}
  - capability: shell
    variant: Bash
    revision: claude-agent-sdk-0.3.316-v1
    arguments: {command: $command, timeout_ms: $timeout}
    refuse: [run_in_background, dangerouslyDisableSandbox]
  - capability: edit
    variant: NotebookEdit
    revision: claude-agent-sdk-0.3.316-v1
    arguments:
      path: $notebook_path
      notebook:
        $case: edit_mode
        $default: replace
        replace: {kind: replace, cell: {kind: id, id: $cell_id}, source: $new_source, cell_type: $cell_type}
        insert:
          $if: cell_id
          then: {kind: insert, cell: {kind: id, id: $cell_id}, after: true, id: $@operation_id, cell_type: $cell_type, source: $new_source}
          else: {kind: insert, cell: {kind: index, index: 0}, id: $@operation_id, cell_type: $cell_type, source: $new_source}
        delete: {kind: delete, cell: {kind: id, id: $cell_id}}
---
