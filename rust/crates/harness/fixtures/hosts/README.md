# Native host output captures

`codex-*.stdout` and `codex-*.stderr` contain the unmodified output of the
official native binaries' `codex --version`, captured on bb2 on 2026-10-25.
`provenance.json` records capture times, npm metadata and tarball URLs, verified
npm SHA-512 integrity, archive/binary/stdout SHA-256, platform and command.
Both commands exited successfully with empty stderr. No host session, model or
credential profile was used.

Capture command: `pwsh -NoProfile -File Q:/lanes/bs.ps1 bash
.bb-plugins-version-capture.sh`, from the plugins worktree. The script fetched
the pinned official npm metadata/tarballs, verified integrity before extracting
the single native executable, and ran it with an explicit minimal environment
and fresh job-owned HOME/CODEX_HOME. TMPDIR was a sibling of HOME. Outputs were
transported as base64 and their recorded hashes verified before writing here.

Build-slot job: `1792927986565-970322-9e0a8566`.
Source clock: `19791456b78c1a395edef5e331166f4b01b57352ac80284d702c92776e42aca1`.
Remote log: `/home/var/log/buildbox/1792927976789-970322-fc1e20b8.log`.
The command source and full dispatch/output receipt are retained at
`D:/lane-scratch/plugins/codex-version-capture-source-20261025.sh` and
`D:/lane-scratch/plugins/codex-version-capture-corrected-20261025.log`.

The shared `host-hooks` test accepts the captured 0.164.0 release and refuses the
captured 0.146.0 release. Malformed-version controls are explicitly derived
mutations of the supported capture, rather than invented external output.

Attribution: Codex <noreply@openai.com>.
