# Git layout refusal controls

Apply each `git-layout-*.patch` separately from the repository root, then run:

```
cargo test --locked -p sdk-generation --lib historical::probe::repository_layout_preserves_original_path_and_refusals -- --exact --nocapture
```

The shallow, tail and empty-path mutations change the actual production parser.
The shared property/fuzz oracle must reject them with `Git layout admitted shallow
ancestry`, `Git layout admitted an extra record`, and `Git layout admitted an empty
path`, respectively. Revert each patch before the next; the unchanged production
parser must pass. Existing borrowed UTF-8 path custody remains the parser result;
no subprocess, repository setup, polling or alternate parser is introduced.
The original fuzzer merely discarded this parser result. These controls qualify
the semantic oracle, not source branch coverage or an instrumented fuzz campaign.
