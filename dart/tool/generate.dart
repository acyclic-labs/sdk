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

String pascalIdentifier(String value) => value
    .split(RegExp(r'[^a-zA-Z0-9]+'))
    .where((part) => part.isNotEmpty)
    .map((part) => '${part[0].toUpperCase()}${part.substring(1)}')
    .join();

String dartValueType(String wireKind) {
  switch (wireKind) {
    case 'string':
      return 'String';
    case 'bytes':
      return 'List<int>';
    case 'signed_integer':
    case 'unsigned_integer':
    case 'enum':
      return 'int';
    default:
      return 'Object?';
  }
}

List<String> dartValidationLines(Map item) {
  final id = item['id'] as String;
  final wireKind = item['wire_kind'] as String;
  final rules = (item['rules'] as List).whereType<Map>().toList();
  final lines = <String>[];
  if (wireKind == 'string') {
    lines.add("if (value.isEmpty) { throw ArgumentError('$id: value must be non-empty'); }");
  } else if (wireKind == 'bytes') {
    lines.add("if (value.isEmpty) { throw ArgumentError('$id: value must be non-empty'); }");
  }
  for (final rule in rules) {
    switch (rule['kind']) {
      case 'fixed_length':
        lines.add("if (value.length != ${rule['length']}) { throw ArgumentError('$id: invalid byte length'); }");
      case 'strictly_positive':
        lines.add("if (value <= 0) { throw ArgumentError('$id: must be positive'); }");
      case 'non_negative':
        lines.add("if (value < 0) { throw ArgumentError('$id: must be non-negative'); }");
      case 'max_items':
        lines.add("if (value > ${rule['max']}) { throw ArgumentError('$id: exceeds maximum'); }");
      case 'bounded_integer':
        lines.add("if (value < ${rule['min']} || value > ${rule['max']}) { throw ArgumentError('$id: outside bounds'); }");
      case 'exact_oneof':
        lines.add("if (value is! Map || value.length != 1) { throw ArgumentError('$id: exactly one arm is required'); }");
    }
  }
  return lines;
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

List<String> protocIncludeRoots(List<String> schemaRoots) {
  final roots = <String>[...schemaRoots];
  final explicit = Platform.environment['PROTOC_INCLUDE'];
  if (explicit != null && explicit.isNotEmpty) {
    roots.add(Directory(explicit).absolute.path);
  }

  final pubCache =
      Platform.environment['PUB_CACHE'] ??
      (Platform.isWindows
          ? '${Platform.environment['LOCALAPPDATA']}${Platform.pathSeparator}Pub${Platform.pathSeparator}Cache'
          : '${Platform.environment['HOME']}${Platform.pathSeparator}.pub-cache');
  final hosted = Directory(
    '$pubCache${Platform.pathSeparator}hosted${Platform.pathSeparator}pub.dev',
  );
  if (hosted.existsSync()) {
    for (final entity in hosted.listSync()) {
      if (entity is Directory &&
          entity.path
              .split(Platform.pathSeparator)
              .last
              .startsWith('protobuf-') &&
          Directory(
            '${entity.path}${Platform.pathSeparator}google',
          ).existsSync()) {
        roots.add(entity.path);
      }
    }
  }
  return roots.toSet().toList();
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
  // Dart can use a platform-specific wrapper when the shared protoc binary
  // lives on a restricted workspace drive; other producers continue to use
  // the canonical PROTOC executable.
  final protoc = executable('PROTOC_DART', executable('PROTOC', 'protoc'));
  final plugin = dartPlugin();
  if (plugin == null) {
    stderr.writeln(
      'PROTOC_GEN_DART must point to protoc-gen-dart; activate protoc_plugin 25.1.0 first',
    );
    exitCode = 2;
    return;
  }
  final protocSkipped = Platform.environment['PROTOC_SKIP'] == '1';
  if (!protocSkipped && output.existsSync()) {
    output.deleteSync(recursive: true);
  }
  output.createSync(recursive: true);
  final protocArguments = [
    for (final schemaRoot in protocIncludeRoots(schemaRoots)) ...[
      '-I',
      schemaRoot,
    ],
    '--plugin=protoc-gen-dart=$plugin',
    '--dart_out=grpc:${output.path}',
    ...schemaFiles,
  ];
  // Windows may deny CreateProcess for an executable on a mapped workspace
  // drive even though the same binary is runnable through the command host.
  // Keep the producer deterministic while using the native Windows launcher.
  if (!protocSkipped) {
    final result = Platform.isWindows
        ? await Process.run(
            Platform.environment['COMSPEC'] ?? 'cmd.exe',
            ['/d', '/c', protoc, ...protocArguments],
            workingDirectory: root.path,
          )
        : await Process.run(protoc, protocArguments, workingDirectory: root.path);
    stdout.write(result.stdout);
    stderr.write(result.stderr);
    if (result.exitCode != 0) {
      exitCode = result.exitCode;
      return;
    }
  } else if (!output.listSync(recursive: true).any(
        (entity) => entity is File && entity.path.endsWith('.pb.dart'),
      )) {
    stderr.writeln(
      'PROTOC_SKIP=1 requires generated Dart protobuf files in ${output.path}',
    );
    exitCode = 2;
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
  if (explicitSchemaRoot == null) {
    stderr.writeln(
      'Rust-emitted schema root is required so generated tests stay bound to Rust-owned fixtures',
    );
    exitCode = 2;
    return;
  }
  final fixtureSource = File(
    '$explicitSchemaRoot${Platform.pathSeparator}rust-family-goldens.json',
  );
  if (!fixtureSource.existsSync()) {
    stderr.writeln('Rust-owned fixture missing: ${fixtureSource.path}');
    exitCode = 2;
    return;
  }
  final fixtureBytes = fixtureSource.readAsBytesSync();
  final fixture = jsonDecode(utf8.decode(fixtureBytes));
  if (fixture is! List || fixture.length != 9) {
    stderr.writeln('Rust-owned fixture must contain nine family goldens');
    exitCode = 2;
    return;
  }
  if (manifestSha256 != null &&
      fixture.any(
        (entry) =>
            entry is! Map ||
            entry['authority_manifest_sha256'] != manifestSha256,
      )) {
    stderr.writeln(
      'Rust-owned fixture is bound to a different authority manifest',
    );
    exitCode = 2;
    return;
  }
  final typePolicyCandidates = schemaRoots
      .map(
        (candidate) => File(
          '$candidate${Platform.pathSeparator}type-policy.json',
        ),
      )
      .where((candidate) => candidate.existsSync())
      .toList();
  final typePolicySource = typePolicyCandidates.isEmpty
      ? null
      : typePolicyCandidates.first;
  if (manifestFile != null && typePolicySource == null) {
    stderr.writeln('Rust-owned type policy missing from schema root');
    exitCode = 2;
    return;
  }
  Map<String, dynamic>? typePolicyMetadata;
  if (typePolicySource != null) {
    final typePolicyBytes = typePolicySource.readAsBytesSync();
    final decoded = jsonDecode(utf8.decode(typePolicyBytes));
    if (decoded is! Map || decoded['schema'] != 'acyclic.sdk.type-policy.v1') {
      stderr.writeln('unexpected Rust type policy schema');
      exitCode = 2;
      return;
    }
    final profiles = decoded['languages'];
    final profileCandidates = profiles is List
        ? profiles.whereType<Map>().where((entry) => entry['language'] == 'dart').toList()
        : <Map>[];
    final profile = profileCandidates.isEmpty ? null : profileCandidates.first;
    if (profile == null || profile['nominal_types'] is! String || profile['refinements'] is! String || profile['unions'] is! String) {
      stderr.writeln('Rust type policy has no Dart profile');
      exitCode = 2;
      return;
    }
    final destination = File('${package.path}${Platform.pathSeparator}type-policy.json');
    destination.writeAsBytesSync(typePolicyBytes);
    final semanticTypes = decoded['semantic_types'];
    if (semanticTypes is! List || semanticTypes.isEmpty) {
      stderr.writeln('Rust type policy semantic_types must be non-empty');
      exitCode = 2;
      return;
    }
    final policySource = <String>[
      '// Generated exclusively from rust/crates/sdk-contract-wire/src/type_policy.rs.',
      '',
    ];
    final negativeTests = <String>[
      "import 'package:test/test.dart';",
      "import '../lib/src/type_policy.dart';",
      '',
      'void main() {',
    ];
    for (final raw in semanticTypes.whereType<Map>()) {
      final className = pascalIdentifier(raw['id'] as String);
      final valueType = dartValueType(raw['wire_kind'] as String);
      policySource.addAll([
        'sealed class $className {',
        '  const $className._();',
        '  $valueType get value;',
        '  factory $className.from($valueType value) => ${className}Value.from(value);',
        '}',
        '',
        'final class ${className}Value extends $className {',
        '  @override final $valueType value;',
        '  const ${className}Value._(this.value) : super._();',
        '  factory ${className}Value.from($valueType value) {',
        ...dartValidationLines(raw).map((line) => '    $line'),
        '    return ${className}Value._(value);',
        '  }',
        '}',
        '',
      ]);
      final rules = (raw['rules'] as List).whereType<Map>().map((rule) => rule['kind']).toSet();
      String? invalid;
      if (rules.contains('non_empty')) {
        invalid = raw['wire_kind'] == 'bytes' ? '<int>[]' : "''";
      } else if (rules.contains('fixed_length')) {
        invalid = raw['wire_kind'] == 'bytes' ? '<int>[0]' : "'x'";
      } else if (rules.contains('strictly_positive')) {
        invalid = '0';
      } else if (rules.contains('non_negative')) {
        invalid = '-1';
      } else if (rules.contains('exact_oneof')) {
        invalid = '<String, Object?>{}';
      }
      if (invalid != null) {
        negativeTests.add("  test('rejects invalid $className', () { expect(() => $className.from($invalid), throwsArgumentError); });");
      }
    }
    negativeTests.addAll(['}', '']);
    File('${package.path}${Platform.pathSeparator}lib${Platform.pathSeparator}src${Platform.pathSeparator}type_policy.dart')
        .writeAsStringSync('${policySource.join('\n')}\n');
    File('${package.path}${Platform.pathSeparator}test${Platform.pathSeparator}type_policy_negative_test.dart')
        .writeAsStringSync('${negativeTests.join('\n')}\n');
    typePolicyMetadata = {
      'path': 'type-policy.json',
      'sha256': sha256.convert(typePolicyBytes).toString(),
      'schema': decoded['schema'],
      'language': 'dart',
      'profile': profile,
      'artifacts': [
        'lib/src/type_policy.dart',
        'test/type_policy_negative_test.dart',
      ],
    };
  }
  final fixtureDestination = File(
    '${package.path}${Platform.pathSeparator}test${Platform.pathSeparator}fixtures${Platform.pathSeparator}rust-family-goldens.json',
  );
  fixtureDestination.parent.createSync(recursive: true);
  fixtureDestination.writeAsBytesSync(fixtureBytes);
  final lock = File(
    '${package.path}${Platform.pathSeparator}generator.lock.yaml',
  ).readAsStringSync();
  final provenance = {
    'generator_lock_sha256': sha256.convert(utf8.encode(lock)).toString(),
    'source_revision': Platform.environment['GIT_COMMIT'] ?? 'unknown',
    'source_git_sha': Platform.environment['GIT_COMMIT'] ?? 'unknown',
    'rust_model_digest':
        Platform.environment['ACYCLIC_RUST_MODEL_DIGEST'] ?? 'unknown',
    'schema_root': explicitSchemaRoot ?? 'diagnostic repository proto roots',
    'schema_inputs_sha256': schemaInputs,
    'rust_family_goldens': fixtureDestination.path
        .substring(package.path.length + 1)
        .replaceAll(Platform.pathSeparator, '/'),
    'rust_family_goldens_sha256': sha256.convert(fixtureBytes).toString(),
    'type_policy': typePolicyMetadata,
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
  final sourceGitSha = provenance['source_git_sha'] as String;
  final modelDigest = provenance['rust_model_digest'] as String;
  if (!RegExp(r'^[0-9a-fA-F]{40}$').hasMatch(sourceGitSha)) {
    stderr.writeln(
      'Dart provenance requires a 40-character Rust source Git SHA (GIT_COMMIT)',
    );
    exitCode = 2;
    return;
  }
  if (!RegExp(r'^[0-9a-fA-F]{64}$').hasMatch(modelDigest)) {
    stderr.writeln(
      'Dart provenance requires the 64-character Rust model digest (ACYCLIC_RUST_MODEL_DIGEST)',
    );
    exitCode = 2;
    return;
  }
  if (authorityManifest?['source_revision'] != null &&
      authorityManifest!['source_revision'] != modelDigest) {
    stderr.writeln(
      'Dart provenance model digest does not match the Rust authority manifest',
    );
    exitCode = 2;
    return;
  }
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
