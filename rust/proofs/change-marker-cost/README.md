# Change marker lookup cost

Run `python3 rust/proofs/qualify.py change-marker-cost` from the repository root.
The retained mutation repeats the actual marker lookup. The existing retained
version and refusal property must reject its measured extra attribute probes,
then pass after byte-identical restoration.

The property uses original v1 bytes and the current changes writer, with the
reserved marker absent, first or last among original and prepared attributes.
Expected probes come from those known fixture positions, without a second
classifier. The same property retains exact wire identity, projection and
unknown-version refusal before parsing. Lookup work is linear in bounded
attribute count; this does not prove whole-request constant cost or platform
acceptance. No production allocation, copy, syscall or wait is added.
