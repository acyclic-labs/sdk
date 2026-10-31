---
name: writing-skills
description: Create or refine a reusable SKILL.md workflow in the harness's discoverable skills directories.
---

Choose a narrow, repeatable task that benefits from instructions beyond the agent's general capabilities. Inspect nearby skills and the existing tool surface before adding another workflow.

Create a folder named for the skill with a SKILL.md containing YAML frontmatter: a lowercase hyphenated name and a description that says what it does and when to use it. Keep the body focused on decisions, required inputs, observable outcomes, and constraints. An instruction skill needs no executable declaration.

When the host supports callable skills and the workflow needs reusable execution, add both input_schema (a JSON Schema object or boolean) and runtime to the frontmatter. Set runtime.kind to a supported shell or code runtime and runtime.script to the script's volume-relative path beneath the discovered skills root. Keep the script with the skill. Code receives arguments as JSON data in input; do not interpolate arguments into source. Invoke an admitted skill through call with its exact tool identity and args. The declaration grants no authority: the host's existing approvals, directory grants and runtime admission still apply.

Delete duplicated prompt instructions and generic advice. Add supporting files only when they supply necessary detail or reliable reusable execution, and link them where needed. Exercise the workflow on representative tasks when permitted, including a case that should not activate it. Separate format validation from evidence that the workflow works; preserve user edits when revising an existing skill.
