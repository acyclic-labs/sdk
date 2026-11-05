# Syntect 5.3.0

Published as `acyclic-syntect`, with the original `syntect` library name.

Production source and license from the crates.io `syntect` 5.3.0 archive,
upstream commit `e4670846ecf16d8832db6c43d531bec466214e27`.
The original grammar and theme data are retained, with their serialization
replaced by the MessagePack implementation already used for SDK records.

Dump format v1 is a four-byte big-endian version followed by one MessagePack
value, using named fields and Serde's human-readable representations. Lazy
context byte buffers use MessagePack binary values. Compression remains zlib;
outer syntax sets remain uncompressed so individual grammars load lazily.
Unknown versions and trailing decoded bytes are rejected. Compression errors
are returned when the encoder finishes. No old-format reader is retained.
File writers flush their buffer once at completion; generic writers remain
owned by their caller. Neither path requests filesystem durability.

All four bundled assets were captured through the original Syntect decoder,
including each of the 75 grammars' actual lazy contexts in both newline modes.
The captured values were transcoded to v1 and checked for value equality.
The Markdown syntax-data corpus copies the resulting production syntax and
theme assets; the shared decoder property is also a fuzz target. Original
upstream tests referencing external test data are not part of this production
source import.
