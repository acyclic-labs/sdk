# Fitted table cell work

`node rust/proofs/qualify.mjs markdown-fitted-width` runs the existing Unicode
table control against a mutation that restores the extra fitted-cell width
scan, then restores the exact source and runs the same unchanged oracle.

The production table still measures each cell for column layout and admission.
Admission now returns its measured width with the admitted line, so padding
reuses that result. Fitted cells require two width scans including layout,
instead of three. Each multi-span scan still joins the original text because
graphemes can cross style boundaries. Removing one scan also removes its
temporary concatenation. Truncated cells retain the original width computation
after clipping; no grapheme-width equivalence is assumed.

The existing control retains every narrow Unicode case. Its fitted cells are
produced by the actual Markdown parser, rendered through the production table,
and compared with the public incremental renderer. Shared deterministic byte
counters require exactly the layout and admission scans. The mutation adds the
removed scan to the real fitted branch and must fail that cost assertion.
The wide-table empty-cell guard property and all chunk histories are unchanged.

The operation remains proportional to visible cell bytes and spans. It adds no
wait, producer, syscall, durability barrier, round trip, or allocation; it removes
one fitted-cell byte scan and, for multi-span cells, one concatenation allocation
and copy. This component control does not establish a whole-suite duration or a
platform-specific speedup.
