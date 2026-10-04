# Python/Go exact-source remote qualification

This receipt set records actual remote gRPC executions against the Rust fixture built from Git revision `b5293335694f19d0132be13542f6925111712433`.

- Rust model authority digest: `6bfa6cd366ebba884a4ad3eba8931dc19140e145aeeb145f9effe1b37bdf4b9a`
- Execution mode: `remote` for every scenario row
- Go: 50/50 Workers, Objects, and Filesystem methods passed; archive SHA-256 `36E31F9F8BB8421CDF96D1378928EF2309FFDF9EAEA764A07566372E3B9867D6`
- Python: 50/50 Workers, Objects, and Filesystem methods passed; wheel SHA-256 `186A8CA9DAEA7C61CC541FB2AECDD26E77FFAC2C8CC6C1ECDBFC354534992A8F`
- Fixture endpoint: `127.0.0.1:57939`, rebuilt from the exact Rust source snapshot

The Go receipt is `go-b529-scenarios.json`; the Python receipt is `python-b529-scenarios-remote.json`. The package archives are copied beside those receipts. Actor and Stream stateful receipts remain in the dedicated exact-source fixture evidence set.
