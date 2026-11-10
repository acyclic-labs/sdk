# Install GraphCoder

GraphCoder's prebuilt executables are published in this repository's
[GitHub Releases](https://github.com/acyclic-labs/sdk/releases). No Rust toolchain
or package manager is needed. Linux and macOS support x86_64 and arm64; Windows
supports x64 and arm64. Linux assets include glibc and musl builds.

Linux or macOS (curl and the system SHA-256 tool):

```sh
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' https://github.com/acyclic-labs/sdk/releases/latest/download/install-graphcoder.sh | sh
```

Windows, in PowerShell 7:

```powershell
Invoke-RestMethod https://github.com/acyclic-labs/sdk/releases/latest/download/install-graphcoder.ps1 | Invoke-Expression
```

The installer prints the installation directory. Add it to `PATH`, then run
`graphcoder --version` and `graphcoder --help`. The default is `~/.local/bin` on
Linux/macOS and `%LOCALAPPDATA%/GraphCoder/bin` on Windows. Set
`GRAPHCODER_INSTALL_DIR` to select another directory. Installation needs write
access there and does not require administrator privileges.

Each installer pins its downloads to its release's `acyclic-vVERSION` tag,
verifies the binary against that release's `SHA256SUMS`, and checks its version
before replacing an existing installation with a same-filesystem rename.
Download or verification failure leaves the previous executable intact. On
Windows, close GraphCoder before replacing a running executable.

To pin a release, replace `latest/download` in the command with
`download/acyclic-vVERSION`. To update, run the latest installer again. To remove
GraphCoder, delete the installed executable; your session files stay available.

For offline installation, download the installer, `SHA256SUMS` and the matching
`graphcoder-TARGET` executable (`.exe` on Windows) from the same release. Set
`GRAPHCODER_ASSET_DIR` to that directory and run the downloaded installer. This
uses the same verification path, and a missing local file fails immediately.
Checksums establish integrity against the selected release inventory. Release
assets also carry GitHub build provenance attestations.

Maintainers build the shipped executable with
`node scripts/build-product.mjs graphcoder`, using the shared `dist` build
recipe. The existing release workflow verifies each target's native image,
exercises installation with real binaries on Linux, macOS and Windows, and
includes all GraphCoder binaries and installers in the attested `SHA256SUMS`.
The offline scenario is `node scripts/test-install-release-asset.mjs`, with
`GRAPHCODER_TEST_BINARY` set to an absolute source-qualified executable.
