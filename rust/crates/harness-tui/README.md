# Terminal presentation

The shared `Screen` draws followed data; it does not read files. Theme overrides
are partial `config/theme.json` files, folded over the shipped semantic tokens.
`mode` is `auto`, `light` or `dark`. Auto follows OSC 11 background luminance;
`/theme light`, `/theme dark` and `/theme auto` write the same configuration.
Code uses those tokens and a background band derived from the negotiated
background and muted token, with ANSI and monochrome fallbacks.

Transcript blocks own their parsed Markdown cache. Streaming appends only the
new suffix to the shared Markdown parser; changing a theme recolors settled
blocks without rerunning the grammar. The transcript has a one-column gutter,
one empty row between messages, and compact tool cards. The prompt has a top
rule and shares its geometry with mouse routing. Chrome is muted and status
hints align to the right.

Code colours are requested only for visible wrapped rows, including byte
windows of very long source lines. Immutable source and grammar-context keys
share those styles across paints and reflows. Stateful grammars parse an
uncached preceding prefix to establish multiline string and comment context;
that scan does not materialize offscreen styles. Caches belong to the viewed
document and retain no superseded streaming preview text.

Links retain their original destinations outside printable text. A linked
`FrameTerminal::draw_linked` return value accompanies the painted cells through
the ordinary retained frame encoder. Destination-only changes and removal
repaint the label; unchanged frames write no bytes. Control characters refuse
a destination. Each frame closes its OSC 8 links; interrupted frames cancel
partial escape sequences and close links through the same restoration journal.

`tests/golden/` contains complete independently interpreted terminal cells for
each shared view, light and dark, at 80×24 and 120×40, and truecolour, indexed,
ANSI and monochrome depths. To regenerate them, run the `golden::` tests with
`UPDATE_GOLDEN=1`. `GOLDEN_OUTPUT` selects a separate export directory. Review
the resulting cell, attribute and destination diffs before committing them.
The same cell oracle rejects original output stripped of styling or links;
the interruption test also removes the restoration journal as a negative
control. Shipping acceptance is `pty-markdown` and `pty-code-highlight`.
