# Composed EOF qualification

The two native-runtime properties compare incremental framing with the async
reader at every read split and preserve the recovered trailing frame after
malformed input. The retained mutation discards the final frame. Qualification
must select both exact properties, observe both original failure diagnostics,
and restore both properties to success.

Run the real-binary integration check with
`node --test rust/proofs/native-line-eof/qualification.test.mjs`.
The runner mutation restores the former single-property selection; the same
integration check must reject its missing second property.
