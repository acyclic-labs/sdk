---
version: 1
tools:
  - capability: read
    variant: read_file
    revision: gemini-cli-6814e00d6b14-v1
    arguments:
      path: $file_path
      selection: {kind: lines, offset: {$one_based: start_line}, limit: {$through: [start_line, end_line]}}
  - capability: write
    variant: write_file
    revision: gemini-cli-6814e00d6b14-v1
    arguments: {path: $file_path, content: $content}
  - capability: edit
    variant: replace
    revision: gemini-cli-6814e00d6b14-v1
    arguments: {path: $file_path, old_text: $old_string, new_text: $new_string, replace_all: $allow_multiple}
  - {capability: glob, variant: glob, revision: gemini-cli-6814e00d6b14-v1}
  - {capability: search, variant: grep_search, revision: gemini-cli-6814e00d6b14-v1}
  - capability: shell
    variant: run_shell_command
    revision: gemini-cli-6814e00d6b14-v1
    arguments: {command: $command, cwd: $dir_path}
    refuse: [is_background, delay_ms]
---
