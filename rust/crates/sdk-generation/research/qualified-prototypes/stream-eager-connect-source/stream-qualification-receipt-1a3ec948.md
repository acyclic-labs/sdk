# Stream native qualification receipt

- Source checkout: `Q:\sdk\work\stream-main-port`
- Signed source revision: `1a3ec9481b6d19722bbc23fbef1f34e52657b355`
- Source tree status: clean
- `rust/crates/stream/src/grpc.rs` blob: `1f1bc320a51c1133348643523b0cf9077b1e6e22`
- Native source closure: `sha256:f1cd7e8afe02172fd8dfeb340c5632f1a6e0684ccbbb79c50914eb27a5dcf72d`
- Closure input manifest: `Q:\sdk\work\stream-native-bundle-1a3ec948\native-targets.json` (`source_files`)
- Bundle: `Q:\sdk\work\stream-native-bundle-1a3ec948`
- Staged package: `Q:\sdk\work\stream-package-1a3ec948`
- Package archive: `Q:\sdk\work\stream-package-archive-1a3ec948\acyclic-labs-stream-0.2.0.tgz`
- Archive SHA256: `bce8fb4bb97455ab462607c86cbb6e29522cd5bd66549ba9a39312a050350379`
- Installed consumer: `Q:\sdk\work\stream-installed-1a3ec948`
- Raw evidence: `Q:\sdk\work\stream-qualification-evidence-1a3ec948`

The bundle, staged package, package source, and installed consumer native artifact are all 5,358,080 bytes with SHA256 `d02a33d19a87bab3226d1113dc7e63eb921bd869bfb9570a50a9d398c5bce4d6`. Loading the installed binding registers exactly the expected `.node` path.

Installed cancellation evidence: `activeSocketCount: 0`, `socketClosedByAbort: true`, `invalidBearerRejectedBeforeNetwork: true`, `unhandledRejections: 0`.

Gates: 55 TypeScript tests passed; 18 planner tests passed; Node and Bun provider conformance passed; focused malformed-bearer and stalled-TLS Rust tests passed; locked `stream-napi` check passed; native/default/browser selector checks passed.