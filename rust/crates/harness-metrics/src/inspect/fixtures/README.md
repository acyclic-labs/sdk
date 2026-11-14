# Inspect reference

`inspect-sdk.json` is real `inspect-ai` 0.3.287 output, captured on 2026-11-02
using `inspect_ai.log.write_eval_log(..., format="json")` and read back with
`inspect_ai.log.read_eval_log`. Its four messages are generated format controls,
not provider traffic or a measured evaluation. The SDK generated the message
identifiers and log defaults.

SHA-256: `74434c375212bf553a16409cf339aaf70fdaa27c1e6cddfe3b5d20f6542c9d48`.

The codec control compares roles, text and tool linkage against this output.
The separate property and allocation controls generate their own input values;
they do not represent captured provider responses.
