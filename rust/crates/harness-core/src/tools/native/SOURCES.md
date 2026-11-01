# Pinned public tool contracts

These declarations adapt public tool interfaces to the shared capability
executors. They do not import a host's agent loop or execution implementation.
The family profile files pin the declaration and adapter revision together.

| Family | Public source | Selection |
| --- | --- | --- |
| Claude Code | [Claude Agent SDK 0.3.316](https://registry.npmjs.org/@anthropic-ai/claude-agent-sdk/-/claude-agent-sdk-0.3.316.tgz), `sdk-tools.d.ts` | `BashInput`, `FileReadInput`, `FileWriteInput`, `FileEditInput`, `GlobInput`, `GrepInput`; field names, types, optionality and field documentation. Tool summaries follow the [public SDK reference](https://code.claude.com/docs/en/agent-sdk/typescript). |
| Codex shell | [Codex rust-v0.20.0](https://github.com/openai/codex/tree/45aed5edd139c38132de044596fdd55e47236657), `codex-rs/core/src/openai_tools.rs` | The plain `create_shell_tool` command-array contract. |
| Codex patch | [Codex 67f0994b](https://github.com/openai/codex/tree/67f0994b532b8740f14b5b738af7b7b2a915b1d6), `codex-rs/core/src/tools/handlers/apply_patch_spec.rs`, `codex-rs/core/assets/tools/apply_patch.lark`, and `codex-rs/apply-patch/src/lib.rs` | The freeform declaration, complete patch grammar, and `print_summary` success format. |
| Gemini CLI | [Gemini CLI 6814e00d](https://github.com/google-gemini/gemini-cli/tree/6814e00d6b148c3a1d57244d5d7040f7c541009d), `packages/core/src/tools/definitions/model-family-sets/gemini-3.ts` | `read_file`, `write_file`, `replace`, `glob`, `grep_search`, `run_shell_command`; base declarations and dynamic description helpers at the same revision. |

The Claude SDK package records source commit
`cd7ceb459c6864567b16a11f60101b24421475c1` in its package manifest. This is
package provenance, not a claim that the implementation is published at that
commit in a public repository. Its public type contracts are interoperability
references; the SDK's proprietary runtime is not included.

Codex and Gemini CLI schema material retains its Apache-2.0 attribution in the
root NOTICE. The JSON files are extracted declarations, not captured model
responses. Adversarial protocol tests generate their own data and do not claim
provider-fixture provenance.

Missing-text and occurrence-count edit errors follow `getErrorReplaceResult`
in Gemini CLI's `packages/core/src/tools/edit.ts` at the same pinned revision.
Only its public diagnostic contract is adapted; matching and publication use
the shared exact edit engine. Overlapping matches remain refused by that
engine's stronger unique-match contract.

Gemini search's default case-insensitive matching follows
`performRipgrepSearch` in `packages/core/src/tools/ripGrep.ts` at that revision.
The adapter selects arguments for the original command executor; it does not
import that tool's process runner, file discovery or context enrichment.

The `golden/` lists are deterministic protocol projections of those extracted
public declarations, with recursively sorted schema keys. They are declaration
goldens, not observations of a provider stream or a private harness prompt.

The Claude Grep `head_limit` description adapts the upstream default to the
harness contract: omission and zero mean unlimited output. Explicit positive
limits still select the original stdout before tail retention.
