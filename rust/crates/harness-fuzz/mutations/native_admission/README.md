# Native declaration admission control

`unchecked-identity.patch` removes the production admission check joining the
mapping's name and revision to its declaration. The existing family binding
control must fail at its mismatched-identity assertion with that mutation.

Run the scoped control after applying the patch and again after reversing it:

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --features filesystem,test-support -E 'test(tools::native::tests::family_shell_binding_keeps_one_selection_and_the_original_capability)'
```

The control derives its declarations and argument mappings from each actual
embedded native family. It edits the description and attempts to replace result
contracts, then checks the production registry retains the original executor
contracts and authority. Missing capabilities remain absent. No provider reply
or external-tool output is fabricated.