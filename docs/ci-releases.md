# CI and SDK releases

Every PR and main push runs `SDK Static`, one five-minute job on standard
GitHub-hosted Ubuntu. It checks Rust formatting, whitespace, JSON and JavaScript
syntax, pinned Actions, runner policy, and compatibility digests. Documentation
changes receive the same required check. Superseded static runs are cancelled.
Builds, tests, generator checks, and native integrations run during manual
qualification or a release. Tags and GitHub Release events start no workflows.

## Coordinated release

Commit the same version in all publishable npm, Cargo, and plugin manifests, then
dispatch **SDK Release** (`release.yml`) from main with that version:

```sh
gh workflow run release.yml --repo acyclic-labs/sdk --ref main \
  -f version=0.2.0 -f publish=false
```

`publish=false` runs the complete release graph without publishing. For the live
release, dispatch with `publish=true`. The dispatch freezes main's SHA. Every
checkout, qualification receipt, plugin archive, and publisher uses that source;
later main commits cannot change the release.

The graph runs full SDK qualification once, supported plugin platform builds,
native mount and host integration, and generator checks. It assembles and attests
the universal plugin and passes explicit retained artifact references to the npm
and Cargo publishers. The registry jobs retain their `npmjs` and `crates-io`
environments. Both registries must trust the calling workflow `release.yml`:
reusable publisher filenames do not preserve their former caller identities.
Configure that workflow for every publishable package and crate before enabling
publication. The validation dispatch requires no registry publication credentials.
Registry version conflicts and mismatching artifact bytes
fail instead of replacing a version. Existing registry integrity and installation
checks remain part of the release.

## Interrupted publication

npm, Cargo, and GitHub cannot commit a release atomically. The run summary reports
each channel's result. A failed publisher can leave some packages published, so
inspect the run before retrying.

Use **Re-run failed jobs** on that same release run. Successful qualification and
build jobs retain their original artifacts and outputs; the remaining publishers
resume those inputs. Registry publishers compare existing packages with the
qualified inputs. Plugin asset uploads accept identical existing assets and
reject conflicting contents. The GitHub Release remains a draft until every
plugin asset has uploaded successfully.

Do not change the version, dispatch a new source, or rerun all build jobs to
recover a partially published release. Qualification artifacts expire after
their configured retention period; missing or expired artifacts fail recovery
instead of silently rebuilding. Qualification retries before publication may
retain successful platform lanes from earlier attempts of the same run. The
assembler freezes their exact artifact names and publishers still validate their
source and contents.

Manual diagnostic qualification remains available in `qualification.yml`,
`native-mount-qualification.yml`, `agent-host-qualification.yml`, and
`sdk-generator.yml`. These diagnostics do not publish a release.

Qualification keeps installed-tool license and protobuf checks ahead of Linux
compilation; generated-surface and package checks still follow their builds.
Strict first-party script checking consumes the pinned YAML parser through an
`unknown`-returning declaration and guarded workflow mappings. Release output
helpers require `GITHUB_OUTPUT` before doing source or artifact work.

The Harness Chrome media-boundary scenario selects `wasm,filesystem`: its test
is filesystem-gated and memory-backed. It does not qualify an IndexedDB provider
or establish that the ordinary browser runtime passes.

## Filesystem native companions

The retained NAPI family is `filesystem`, not a Workerd service. Its Rust-owned
matrix requires GNU Linux x64/ARM64, Darwin x64/ARM64, and MSVC Windows x64/ARM64.
The 0.2.0 parent declares the six `@acyclic-labs/fs-*` optional packages using
the canonical NAPI `-gnu`/`-msvc` selectors. The generated loader is the only
native resolution path.

Each lane builds `build-filesystem-native.mjs`, retains the original compiler
receipt, builds the public distribution, and runs `check-filesystem-napi.mjs
--bundle ABSOLUTE_BUNDLE --producer-receipt ABSOLUTE_RECEIPT --adapter`.
The shared assembler packs the exact private binary copy and parent archives;
installation exercises their real `createRequire` loader and the shared native
workspace model. The runtime receipt binds source closure, selected artifact
digest, compiler receipt, actual Node architecture, and both tested archives.
The six-target assembler and publisher reject absent/mismatched runtime receipts,
artifacts, source, package metadata, and checksums; companions publish before
the neutral parent.

It also retains the exact privately tested bytes as
`acyclic-fs-0.2.0-<platform>-<arch>.node` with `SHA256SUMS`; this asset is not a
loader alias. The companion uses the maintained NAPI filename for those same
bytes. Runtime receipts accept actual Node 24 only, not Bun's Node-version
emulation, and require distinct canonical parent/companion archives.

Linux ARM64 now retains source-bound native output. Darwin Intel addons run
under checksum-pinned Intel Node and Bun via Rosetta, not ARM Node. Windows
ARM64 uses the narrowly scoped `windows-11-arm` lane and ARM64 Node; x64 Bun
there is installation tooling only. GitHub documents this runner for private
repositories in its [hosted-runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Cross-compilation or a local Linux pass is not six-platform qualification.
All six same-source CI bundles, original receipts, tested archives and runtime
receipts must be retained before manual publication can proceed.
