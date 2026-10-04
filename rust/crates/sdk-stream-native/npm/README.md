# Platform package fixtures

Each directory is an installable platform package shape for the native Stream bridge. The
publisher copies the matching `acyclic_stream_native.node` binary beside `index.js`; package
metadata restricts installation to the platform and architecture that produced that binary.

The current runtime test qualifies only the Windows x64 binary. The remaining five fixtures
are checked for package metadata and loader shape until matching target binaries are built.
