# Third-party package licenses

The generated package is Apache-2.0. Runtime and generator inputs retain
their upstream licenses. Package versions and repository sources are pinned
by `composer.lock` and `generator.lock.json`.

| Package/tool | Version | License | Upstream source |
| --- | --- | --- | --- |
| grpc/grpc runtime | 1.82.0 | Apache-2.0 | https://github.com/grpc/grpc-php/tree/v1.82.0 |
| google/protobuf runtime | 5.36.2 | BSD-3-Clause | https://github.com/protocolbuffers/protobuf-php/tree/v5.36.2 |
| grpc_php_plugin generator | 1.82.0 | Apache-2.0 | https://github.com/grpc/grpc |
| protoc compiler | exact version required by `generator.lock.json` | BSD-3-Clause | https://github.com/protocolbuffers/protobuf |
