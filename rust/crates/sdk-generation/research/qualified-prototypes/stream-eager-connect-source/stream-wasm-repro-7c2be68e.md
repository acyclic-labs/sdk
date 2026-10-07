# Stream WASM generation reproducibility: 7c2be68e

Audit only; no frozen source edits were made by this audit.

- Checkout: `Q:/sdk/work/stream-main-port`
- Exact signed revision: `7c2be68e75f966f0dccbe0cfef44e79f1195c9fc`
- Producer command: `node scripts/build-wasm.mjs stream Q:/sdk/work/stream-wasm-repro-7c2be68e`
- Producer target: the script's existing `stream-wasm` Cargo target directory with `wasm-release` and pinned wasm-bindgen.

The temporary output matched every tracked Stream WASM output byte-for-byte:

| file | bytes | SHA256 |
| --- | ---: | --- |
| `acyclic_stream_wasm_bg.wasm` | 802,418 | `b2f3c22ba3401dd1e363d8bab17ea758b5870ba25b15bb724abb773fa7772db8` |
| `acyclic_stream_wasm.d.ts` | 10,057 | `215cb17cdb7e0e900e988f5807f4551c7a836f4646b744c92dfa50d491cdff5b` |
| `acyclic_stream_wasm.js` | 29,118 | `5070704d5d1fcd3f1ba5ea2402d0c195cd7e483bf4a8cbc9fac2ad3e58f563a8` |
| `acyclic_stream_wasm_bg.wasm.d.ts` | 3,028 | `c70829be91aa8ab6835a2e2bf2efd6eb2c25d40a40fb760a7a43f5edc2419e96` |

The same WASM SHA256 was independently recorded for the source checkout, staged package, archive extraction, and installed consumer in `stream-qualification-evidence-7c2be68e/wasm-hashguard.txt`. The native package manifest is independently bound to source revision `7c2be68e...` and closure `sha256:f1cd7e8a...`; the archive and installed native artifact hashes are in the corresponding 7c receipt.

The older 5c/ed2 package archives contained the previous 800,761-byte generated WASM (`sha256:523f0b6e...`) while the clean 7c producer output is 802,418 bytes. The difference was a stale committed generated output, not a protocol-source change; after root committed the fresh generated output in 7c, the producer, source tree, staged package, archive, and install all agree. No stale producer output remains in the qualified 7c package.
