# Native TAR corpus

Captured from tar (GNU tar) 1.35 on Windows x86_64, 2026-11-03.

Command, run in the input directory:

```sh
tar --format=ustar --blocking-factor=1 --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf native-ustar.tar body.txt empty.txt nested-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Inputs: empty.txt (empty), body.txt (UTF-8 bytes for native TAR payload, newline, NUL, e-acute, newline), and nested-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/long-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy.txt (UTF-8 prefix member plus newline). The directory and long member exercise USTAR prefix/name composition. Header ownership and timestamps come from the recorded command options.

The archive is original tool output, without header rewriting. SHA-256: 4305b63f5fe98d88e37ad8111785f437ce52ea407bf26964284a4ec1d4315fd4.

The companion native-ustar.list is verbatim stdout from `tar -tf scripts/test-support/archive-corpus/native-ustar.tar` with the same GNU tar, without normalization. It provides the independent member-name oracle.

The companion native-ustar.tar.gz is original gzip 1.13 output captured on Windows x86_64, 2026-11-03, using `gzip -n -c` with native-ustar.tar on stdin. No gzip bytes were rewritten. Length: 215 bytes; SHA-256: c68b93fc12212b2fdd883ec4fd0530d2d0e2a97a2d85463553365555a6e7e29d.

The regular-file-only native-flat.tar.gz was captured with the same GNU tar
1.35 and gzip 1.13 on Windows x86_64, 2026-11-03. Extract native-ustar.tar, run
the original tar command above with only `body.txt empty.txt` as members, then
pass that original TAR output to `gzip -n -c`. No archive bytes were rewritten.
Length: 128 bytes; SHA-256:
0529257d2a1b4a65db39dace19731182fe7f3485a9ad544118c59c6650c6c38b.
native-flat.list is verbatim `tar -tf` stdout. native-flat.files.json records
Node's SHA-256 of the actual extracted files, keyed by that native listing.
The directory and duplicate-member negative inputs are explicit mutations of
these retained native archives, compressed through Node's gzip writer.
