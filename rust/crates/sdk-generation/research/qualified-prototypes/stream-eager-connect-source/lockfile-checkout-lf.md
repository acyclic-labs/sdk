# Source-attested lockfile checkout

Signed candidate: `5c91a9ab5c8fbfd6f50505b01a462d651a4f1d75`.

The candidate adds `*.lock text eol=lf` to `.gitattributes`. Both lockfiles
already contain LF in the committed tree; this makes the working bytes
independent of the consumer's Git automatic CRLF setting.

On Windows, a fresh `git -c core.autocrlf=true checkout-index` into
`Q:/sdk/work/stream-lockfile-lf-probe/` produced bytes identical to the source
checkout. Neither output contained carriage returns. SHA256:

- Cargo.lock: `8D4FCCCF1705DB67559A1473185A2FF1F2D38B5F2961921CB38814CADE31AE20`
- bun.lock: `9EACD1C79F819091FD70C2CADCFC564C378CDE02244CB1A4F29333E2442D5193`

`git check-attr text eol` reports `text: set` and `eol: lf` for both files.
Native package source attestation continues to compare exact bytes rather than
normalizing a potentially modified source file after compilation.
