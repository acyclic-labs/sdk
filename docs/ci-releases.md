# CI and SDK releases

Every PR and main push runs `SDK Static`, one required job on standard
GitHub-hosted Ubuntu. It runs `cargo update --workspace --locked` to check
manifest/lockfile consistency without compiling. The job has a one-minute timeout;
the initial GitHub run completed in seven seconds. Documentation changes receive
the same required check. Superseded static runs are cancelled.

Formatting, whitespace, JSON and JavaScript syntax, pinned Actions, runner policy,
compatibility digests, builds, tests, generator checks, and native integrations
run only through **SDK Release**. Whitespace checks cover changes since the previous
`acyclic-v*` release tag, or the tracked tree for the first release. Tags and GitHub
Release events start no workflows.

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

For diagnostics, dispatch **SDK Release** from main with `publish=false` using
the command above. This runs qualification, native mounts, agent hosts, and
generator checks through the release graph without publishing. Those reusable
workflows have no independent manual triggers.
