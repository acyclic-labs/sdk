# Native host output captures

`codex-*.stdout` and `codex-*.stderr` contain the unmodified output of the
official native binaries' `codex --version`, captured on Linux on 2026-10-25.
`provenance.json` records capture times, npm metadata and tarball URLs, verified
npm SHA-512 integrity, archive/binary/stdout SHA-256, platform and command.
Both commands exited successfully with empty stderr. No host session, model or
credential profile was used.

The capture fetched the pinned official npm metadata and tarballs, verified integrity before extracting the native executable, and ran it with an explicit minimal environment and fresh HOME/CODEX_HOME. The recorded hashes verify the transported outputs.
