import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';

final package = Directory.current;
final root = package.parent;
final output = Directory(
  '${package.path}${Platform.pathSeparator}lib${Platform.pathSeparator}src${Platform.pathSeparator}generated',
);

String executable(String key, String fallback) {
  final value = Platform.environment[key];
  return value == null || value.isEmpty ? fallback : value;
}

String? dartPlugin() {
  final explicit = Platform.environment['PROTOC_GEN_DART'];
  if (explicit != null && explicit.isNotEmpty) {
    return explicit;
  }
  final pubCache =
      Platform.environment['PUB_CACHE'] ??
      (Platform.isWindows
          ? '${Platform.environment['LOCALAPPDATA']}${Platform.pathSeparator}Pub${Platform.pathSeparator}Cache'
          : '${Platform.environment['HOME']}${Platform.pathSeparator}.pub-cache');
  final candidates = Platform.isWindows
      ? <String>[
          '$pubCache${Platform.pathSeparator}protoc-gen-dart.bat',
          '$pubCache${Platform.pathSeparator}protoc-gen-dart.exe',
          '$pubCache${Platform.pathSeparator}bin${Platform.pathSeparator}protoc-gen-dart.bat',
          '$pubCache${Platform.pathSeparator}bin${Platform.pathSeparator}protoc-gen-dart.exe',
        ]
      : <String>[
          '$pubCache${Platform.pathSeparator}bin${Platform.pathSeparator}protoc-gen-dart',
        ];
  for (final candidate in candidates) {
    if (File(candidate).existsSync()) {
      return candidate;
    }
  }
  return null;
}

Future<void> main(List<String> arguments) async {
  final explicitIndex = arguments.indexOf('--schema-root');
  final explicitRaw = explicitIndex >= 0 && explicitIndex + 1 < arguments.length
      ? arguments[explicitIndex + 1]
      : null;
  final explicitSchemaRoot = explicitRaw == null
      ? null
      : Directory(explicitRaw).absolute.path;
  final manifestIndex = arguments.indexOf('--manifest');
  final manifestRaw = manifestIndex >= 0 && manifestIndex + 1 < arguments.length
      ? arguments[manifestIndex + 1]
      : null;
  final manifestFile = manifestRaw == null ? null : File(manifestRaw).absolute;
  Map<String, dynamic>? authorityManifest;
  if (manifestFile != null && manifestFile.existsSync()) {
    final decoded = jsonDecode(manifestFile.readAsStringSync());
    if (decoded is Map<String, dynamic>) {
      authorityManifest = decoded;
    }
  }
  final families = authorityManifest?['families'] is List
      ? (authorityManifest!['families'] as List).whereType<Map>().toList()
      : <Map>[];
  var schemaFiles = families
      .map((family) => family['source'])
      .whereType<String>()
      .toList();
  if (schemaFiles.isEmpty) {
    schemaFiles = authorityManifest?['schemas'] is List
        ? (authorityManifest!['schemas'] as List).whereType<String>().toList()
        : <String>['actors/v1/actors.proto', 'stream/v2/stream.proto'];
  }
  final expectedSchemaHashes = <String, String>{};
  final expectedDescriptorHashes = <String, String>{};
  for (final family in families) {
    final source = family['source'];
    final sourceHash = family['source_sha256'];
    final descriptor = family['descriptor'];
    final descriptorHash = family['descriptor_sha256'];
    if (source is String && sourceHash is String) {
      expectedSchemaHashes[source] = sourceHash;
    }
    if (descriptor is String && descriptorHash is String) {
      expectedDescriptorHashes[descriptor] = descriptorHash;
    }
  }
  final schemaRoots = explicitSchemaRoot == null
      ? <String>[
          '${root.path}${Platform.pathSeparator}proto',
          '${root.path}${Platform.pathSeparator}rust${Platform.pathSeparator}crates${Platform.pathSeparator}stream${Platform.pathSeparator}proto',
        ]
      : <String>[explicitSchemaRoot];
  final dependencyFiles = <String>[];
  for (final schemaRoot in schemaRoots) {
    final directory = Directory(schemaRoot);
    if (!directory.existsSync()) {
      continue;
    }
    for (final entity in directory.listSync(recursive: true)) {
      if (entity is File && entity.path.endsWith('.proto')) {
        dependencyFiles.add(
          entity.path
              .substring(schemaRoot.length + 1)
              .replaceAll(Platform.pathSeparator, '/'),
        );
      }
    }
  }
  schemaFiles = {...schemaFiles, ...dependencyFiles}.toList();
  if (explicitSchemaRoot == null) {
    stderr.writeln(
      'diagnostic fallback: using repository proto roots; pass --schema-root <rust-emitted-root> for qualification',
    );
  }
  final protoc = executable('PROTOC', 'protoc');
  final plugin = dartPlugin();
  if (plugin == null) {
    stderr.writeln(
      'PROTOC_GEN_DART must point to protoc-gen-dart; activate protoc_plugin 25.1.0 first',
    );
    exitCode = 2;
    return;
  }
  if (output.existsSync()) {
    output.deleteSync(recursive: true);
  }
  output.createSync(recursive: true);
  final result = await Process.run(protoc, [
    for (final schemaRoot in schemaRoots) ...['-I', schemaRoot],
    '--plugin=protoc-gen-dart=$plugin',
    '--dart_out=grpc:${output.path}',
    ...schemaFiles,
  ], workingDirectory: root.path);
  stdout.write(result.stdout);
  stderr.write(result.stderr);
  if (result.exitCode != 0) {
    exitCode = result.exitCode;
    return;
  }
  final schemaInputs = <String, String>{};
  for (final relative in schemaFiles) {
    final candidates = schemaRoots
        .map(
          (candidate) => File(
            '$candidate${Platform.pathSeparator}${relative.replaceAll('/', Platform.pathSeparator)}',
          ),
        )
        .where((candidate) => candidate.existsSync())
        .toList();
    if (candidates.isEmpty) {
      stderr.writeln('schema input missing: $relative');
      exitCode = 2;
      return;
    }
    schemaInputs[relative] = sha256
        .convert(candidates.first.readAsBytesSync())
        .toString();
    final expected = expectedSchemaHashes[relative];
    if (expected != null && expected != schemaInputs[relative]) {
      stderr.writeln('schema hash mismatch: $relative');
      exitCode = 2;
      return;
    }
  }
  for (final entry in expectedDescriptorHashes.entries) {
    final candidates = schemaRoots
        .map(
          (candidate) => File(
            '$candidate${Platform.pathSeparator}${entry.key.replaceAll('/', Platform.pathSeparator)}',
          ),
        )
        .where((candidate) => candidate.existsSync())
        .toList();
    if (candidates.isEmpty) {
      stderr.writeln('descriptor input missing: ${entry.key}');
      exitCode = 2;
      return;
    }
    final actual = sha256
        .convert(candidates.first.readAsBytesSync())
        .toString();
    if (actual != entry.value) {
      stderr.writeln('descriptor hash mismatch: ${entry.key}');
      exitCode = 2;
      return;
    }
  }
  final manifestSha256 = manifestFile != null && manifestFile.existsSync()
      ? sha256.convert(manifestFile.readAsBytesSync()).toString()
      : null;
  final lock = File(
    '${package.path}${Platform.pathSeparator}generator.lock.yaml',
  ).readAsStringSync();
  final provenance = {
    'generator_lock_sha256': sha256.convert(utf8.encode(lock)).toString(),
    'source_revision': Platform.environment['GIT_COMMIT'] ?? 'unknown',
    'schema_root': explicitSchemaRoot ?? 'diagnostic repository proto roots',
    'schema_inputs_sha256': schemaInputs,
    if (manifestFile != null) ...{
      'authority_manifest': manifestFile.path.replaceAll(
        Platform.pathSeparator,
        '/',
      ),
      if (manifestSha256 != null) 'authority_manifest_sha256': manifestSha256,
      if (authorityManifest?['schema'] != null)
        'authority_manifest_schema': authorityManifest!['schema'],
      if (authorityManifest?['source_revision'] != null)
        'authority_source_revision': authorityManifest!['source_revision'],
      if (authorityManifest?['exporter'] != null)
        'authority_exporter': authorityManifest!['exporter'],
    },
    'generated_files':
        output
            .listSync(recursive: true)
            .whereType<File>()
            .map(
              (file) => file.path
                  .substring(package.path.length + 1)
                  .replaceAll(Platform.pathSeparator, '/'),
            )
            .toList()
          ..sort(),
  };
  final barrel =
      output
          .listSync(recursive: true)
          .whereType<File>()
          .where((file) => file.path.endsWith('.pbgrpc.dart'))
          .map((file) {
            final symbols =
                RegExp(r'(?:abstract )?class (\w+(?:Client|ServiceBase))')
                    .allMatches(file.readAsStringSync())
                    .map((match) => match.group(1)!)
                    .toSet()
                    .toList()
                  ..sort();
            final path = file.path
                .substring(output.parent.path.length + 1)
                .replaceAll(Platform.pathSeparator, '/');
            return symbols.isEmpty
                ? "export '$path';"
                : "export '$path' show ${symbols.join(', ')};";
          })
          .toList()
        ..sort();
  File(
    '${package.path}${Platform.pathSeparator}lib${Platform.pathSeparator}src${Platform.pathSeparator}generated.dart',
  ).writeAsStringSync('${barrel.join('\n')}\n');
  File(
    '${output.path}${Platform.pathSeparator}provenance.json',
  ).writeAsStringSync(
    '${const JsonEncoder.withIndent('  ').convert(provenance)}\n',
  );
}
