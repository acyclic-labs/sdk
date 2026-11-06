The public failed-input codec must preserve the original guest cause, captured
input and committed disposition. `python3 rust/proofs/qualify.py failed-input`
changes only the production decoder's recovered cause and requires the existing
public Store disposition property to fail its original-cause assertion. The
runner restores exact production bytes and requires the same property to pass
against MemoryStream and LocalStream.

The `actor_codecs` fuzz target decodes arbitrary Stream wire records through
`FailureEntry::decode`. Its shared writer oracle captures committed failed-input,
replay and purge records from `Store::prepare_failed_input` and
`Store::prepare_disposition`; it checks cause, source, subscription, original
captured input and exact disposition against the supplied publication intent.
Corpus bytes come from the existing capture example, never a parallel encoder.
