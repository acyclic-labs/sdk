# Native model settings admission

`native_model_selection` includes the original parser called by
`SettingsConfig::configured_model`. It accepts arbitrary TOML bytes and checks
selected identity against the original document. Generated settings use the
original TOML serializer with arbitrary quoted model names, explicit providers,
opaque slash names and canonical replay. Fixed inputs require both valid
admission and malformed/type/empty/provider refusals; reject-all cannot pass.

The retained corpus is authored user configuration, not external host output.
The target subsumes the prior pure quoted-settings and opaque-name properties.
The real filesystem/root property remains: pure decoding cannot establish
which file was read or its native I/O failures.

The kept control changes the actual production explicit-provider branch to
discard the configured provider. The unchanged identity oracle must fail;
source bytes are restored before corpus replay. This replaces the prior
test-only fault switch. Run from the repository root:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py native_model_selection --runs 1000
```

Corpus replay and one killed mutation do not establish complete source branch
coverage, a campaign, filesystem admission or three-platform qualification.
