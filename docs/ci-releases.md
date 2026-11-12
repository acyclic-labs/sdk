# CI and SDK releases

Every PR and main push runs `SDK Static`, one required Linux job that checks
manifest and lockfile consistency without compiling. It also supports manual
dispatch with `gh workflow run static.yml --ref <branch>`. Run
`node scripts/check-cargo-lock.mjs` locally to reproduce its manifest and lockfile
check. The checker compares the lockfile with Cargo's own canonical output from
`cargo update --workspace`, preserving locked dependency versions while avoiding
dependency source downloads for metadata. If it changes the file, review and
commit the result. The job has a one-minute timeout. Documentation changes receive
the same required check. Superseded static runs are cancelled.

Formatting, whitespace, JSON and JavaScript syntax, pinned Actions, runner policy,
compatibility digests, builds, tests, generator checks, and native integrations
run only through **SDK Release**. Whitespace checks cover changes since the previous
`acyclic-v*` release tag, or the tracked tree for the first release. Tags and GitHub
Release events start no workflows.

Workflow validation requires actionlint 1.7.7 and an executable ShellCheck on
PATH. Use the same wrapper as release policy qualification locally:

```sh
bash scripts/check-workflows.sh
```

For a scoped check, pass the actionlint executable and workflow paths:

```sh
bash scripts/check-workflows.sh actionlint .github/workflows/release-acyclic.yml
```

The wrapper fails when ShellCheck is missing and passes its resolved executable
to actionlint explicitly, so a successful check includes shell validation.

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

The same platform builds also produce standalone GraphCoder executables through
`node scripts/build-product.mjs graphcoder`. Assembly admits the complete target
inventory using the shared native-image verifier. The binaries and version-pinned
shell/PowerShell installers join the existing attested `SHA256SUMS` and GitHub
publisher; the SBOM includes GraphCoder's dependency closure. Native Linux, macOS
and Windows release jobs exercise the offline installer with their built binary,
including replacement and refusal controls. See [Install GraphCoder](graphcoder-install.md).

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

For diagnostics, dispatch **SDK Release** from main with `publish=false` using
the command above. This runs qualification, native mounts, agent hosts, and
generator checks through the release graph without publishing. Those reusable
workflows have no independent manual triggers.

## Release build reuse

`build-product.mjs` and the release workflow's qualifier and Cargo-install
commands use `scripts/release-build.mjs`. Each invocation emits a terminal
`RELEASE_BUILD_RESULT` JSON line containing its action, PASS/FAIL and wall
seconds, including failures and cache hits. Architecture/runtime ABI checks,
installed executable checks, native mounts and assembly still run after the
builds. A restored executable is build evidence; it is not a native-mount pass.

A host can opt into the shared artifact cache by setting
`SDK_RELEASE_BUILD_CONTEXT` to an admitted `artifact-build-context.v2` file.
The adapter loads the absolute module named by `SDK_ARTIFACT_BUILD_MODULE`. Missing or invalid admitted infrastructure fails
explicitly. Without a context, the original compiler command runs.

The context declares a builder digest for OS, dependency sources and external
configuration, a measured `toolchain` declaration, the entire compiler process
environment's allowed names, and every mutable/generated input outside the
clean Git tree. The shared runner executes with only that environment and keys
the action on the tree, builder, actual environment values, command/arguments,
output path, adapter bytes and additional input contents. Its builder digest
does not substitute for toolchain measurement. The shared runner resolves the
selected rustc (including rustup), hashes its bytes and `-vV`, host and target
sysroot rlibs, compiler driver libraries, and the target specification. It hashes
the declared cc/linker drivers and executables selected by their dry runs,
including the backend linker. The declared drivers are pinned through `RUSTC`,
`CC` and Cargo target linker environment variables. Wrapper dependencies outside
those probes must be enumerated in `toolchain.executables`.

`toolchain` contains absolute `rustc`, `cc` and `linker` paths, a `target` triple
or JSON specification path, and optional additional `executables`. File hashes
are retained by resolved path, device, inode, size, nanosecond mtime/ctime and mode;
an unchanged file costs a stat rather than reading its bytes again. Driver probes
and target specifications are measured each time. Measurement repeats before
publication; toolchain installation must share the producer lease to exclude
writes during lookup and execution. The host must decline caching when it cannot establish
that closure. Only successful builds publish artifacts through the existing
shared action cache; failed builds never publish a PASS.

The host retains one leased checkout and compatible target directory for each
platform/profile/flag closure. Release and artifact requests use the shared
`release-dist` pool family, with a distinct dist compatibility identity. The
canonical profile environment clears inherited Cargo profile and Rust flag
overrides before execution; profiles and linker flags stay in repository config.
The shared artifact runner partitions compiled units within that target by the
measured toolchain key: Cargo only fingerprints compiler version output, which
cannot detect every in-place binary or sysroot update. It maps declared target
paths in the command to that partition and returns the executable at the recipe
output path. Existing pool locks and content maps retain targets and restore source clocks
across jobs. Source updates preserve unchanged file mtimes;
Cargo then rebuilds only affected units on a cache miss. Product builds honor
`CARGO_TARGET_DIR`; qualifier and install builds retain their separate target
directories. The installation destination is stable within that lease, so its
temporary name does not change the action key on every request.

The artifact declaration conservatively uses the complete source tree. It can
reuse a retry or another request for identical inputs; unrelated source edits
still enter Cargo's incremental dependency check. A narrower artifact closure
requires evidence of every build-script and generated input before adoption.
Builder admission and persistent target provisioning belong to shared host
infrastructure and require their own installed configuration.

## Dependency preservation

The PR335 JavaScript updates apply to the current workspace, without replacing
the corrected qualification and coordinated publication workflows:

- `@bufbuild/protoc-gen-es`, protobuf and its generator plugin: 2.16.0.
- `@napi-rs/cli`: 3.10.8; Node types: 26.6.4.
- Connect, Connect Node and Connect Web: 2.2.0.
- `bun-types`: 1.4.2; `fake-indexeddb`: 6.2.5.
- The existing React/Svelte dependencies, Pi host 1.1.0 pin, Cargo graph and
  source-generation policy are unchanged by these JavaScript updates.

Lock resolver entries and integrity strings come from the
[PR335 lock delta](https://github.com/acyclic-labs/sdk/pull/335/files), including
`undici-types` 8.9.0 and the nested `bun-types` Node types 26.6.5. The published
[@napi-rs/cli 3.10.8 artifact](https://registry.npmjs.org/@napi-rs/cli/-/cli-3.10.8.tgz)
still constructs `Error` in `dist/index.cjs`, `dist/index.js` and
`src/api/templates/js-binding.ts`. Its new `createLoadErrorChain` preserves
messages in `cause`, not original errors in `AggregateError.errors`. Therefore
the existing three-line patch is retained and rebased to that exact artifact;
it must not be removed merely because upstream now provides a cause chain.

Maintained workflows and composite actions use Checkout 7.0.1, Setup Node
7.1.0, Upload Artifact 7.0.2, Download Artifact 8.0.2, Cache 6.1.0 and Setup Go
7.0.0. Workflow names, callers, qualification lanes and retained-artifact
contracts are preserved; no historical publication workflow is restored.

After integrating source changes, resolve the lock with
`bun install --lockfile-only`, then install with `bun install --frozen-lockfile`.
Run `bun run generate` and inspect generated changes before
`bun run check:generated`, `bun run test` and `bun run test:contracts:browser`;
the last command now builds Actors, Objects and Stream WASM before compiling
and exercising the Actors browser client. These are integration qualification
steps, not evidence of checks executed while porting the dependency updates.
