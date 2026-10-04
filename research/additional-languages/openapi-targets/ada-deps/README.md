# Pinned Ada generator dependencies

The OpenAPI Generator Ada output imports the `utilada_*` and `security` GPR
projects.  This directory records the exact Alire manifest used to provision
those projects.  The lock resolves the following dependency set:

| crate | version | source revision |
| --- | --- | --- |
| `utilada` | 2.9.0 | `bd635f55729b99ca1d2633fb487fe17f935e7d6b` |
| `utilada_xml` | 2.9.0 | `bd635f55729b99ca1d2633fb487fe17f935e7d6b` |
| `utilada_curl` | 2.9.0 | `bd635f55729b99ca1d2633fb487fe17f935e7d6b` |
| `security` | 1.6.0 | `9ca2987053dd91f354837deb4ec69fee5f799267` |
| `xmlada` | 25.0.0 | release archive `v25.0.0` |
| `libcurl` | 7.81.0 | system package `libcurl4-openssl-dev` |

The lock also records Alire's selected GNAT 16.1.0 toolchain.  The existing
qualification worker uses system GNATMAKE 10.5.0 and GPRbuild 18.0w; the
Alire toolchain remains pinned for a clean reproducibility lane and should not
be silently substituted during a qualification run.

Manifest SHA-256: `c4f9e4800e9c0e17647464e1ffeaad92bbc3d66f4e1c273a777f1c5cd80e1ebf`

Lock SHA-256: `47f54234bb091627d2259135a4d0f3828b66a73daf5b6fd4fef894f35cb8a41b`

The lock was resolved by Alire 2.1.1.  The dependency sources were fetched
under `/root/.local/share/alire/builds` and exposed through Alire's generated
`GPR_PROJECT_PATH`; `utilada_sys` is the project file exported by `utilada`,
not a separate crate.

The reproducible bootstrap is `../provision-ada-deps.ps1`.  It copied this
manifest and lock into `/opt/acyclic-runtimes/ada-deps-pinned`, built a small
GPR project importing all four generated dependency projects, and completed
successfully in 11.69 seconds.  Its receipt was
`/opt/acyclic-runtimes/ada-deps-pinned/ada-deps-receipt.json` with SHA-256
`bbb55c0b29be5998e47aa2eef5c0cd95041e457153e0f45599c33f785dc88425`.
