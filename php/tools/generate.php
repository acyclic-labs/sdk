<?php

declare(strict_types=1);

$root = dirname(__DIR__);
$output = $root . DIRECTORY_SEPARATOR . 'src';
$lock = json_decode((string) file_get_contents(__DIR__ . '/../generator.lock.json'), true, 512, JSON_THROW_ON_ERROR);

function optionValue(array $arguments, string $name): ?string
{
    $index = array_search($name, $arguments, true);
    if ($index === false) {
        return null;
    }
    $value = $arguments[$index + 1] ?? null;
    if ($value === null || str_starts_with($value, '--')) {
        throw new InvalidArgumentException($name . ' requires a value');
    }
    return $value;
}

function sha256File(string $path): string
{
    $digest = hash_file('sha256', $path);
    if ($digest === false) {
        throw new RuntimeException('unable to hash ' . $path);
    }
    return $digest;
}

if (($lock['generator_version'] ?? null) !== '1.82.0') {
    throw new RuntimeException('generator.lock.json has an unexpected grpc_php_plugin version');
}

$schemaRootOption = optionValue(array_slice($argv, 1), '--schema-root');
$manifestOption = optionValue(array_slice($argv, 1), '--manifest');
$schemaRoots = $schemaRootOption === null
    ? [$root . '/proto', $root . '/rust/crates/stream/proto']
    : [realpath($schemaRootOption) ?: throw new RuntimeException('schema root does not exist: ' . $schemaRootOption)];
$manifestPath = $manifestOption === null ? null : (realpath($manifestOption) ?: throw new RuntimeException('authority manifest does not exist: ' . $manifestOption));
$authority = $manifestPath === null ? null : json_decode((string) file_get_contents($manifestPath), true, 512, JSON_THROW_ON_ERROR);
$families = is_array($authority['families'] ?? null) ? $authority['families'] : [];
$schemaNames = array_values(array_filter(array_map(
    static fn (mixed $family): ?string => is_array($family) && is_string($family['source'] ?? null) ? $family['source'] : null,
    $families,
)));
if ($schemaNames === []) {
    $schemaNames = is_array($authority['schemas'] ?? null) && $authority['schemas'] !== []
        ? array_values($authority['schemas'])
        : ['actors/v1/actors.proto', 'stream/v2/stream.proto'];
}
$dependencyNames = [];
foreach ($schemaRoots as $candidate) {
    if (!is_dir($candidate)) {
        continue;
    }
    $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($candidate, FilesystemIterator::SKIP_DOTS));
    foreach ($iterator as $path) {
        if ($path->isFile() && str_ends_with($path->getFilename(), '.proto')) {
            $dependencyNames[] = str_replace('\\', '/', substr($path->getPathname(), strlen($candidate) + 1));
        }
    }
}
$schemaNames = $manifestPath === null
    ? array_values(array_unique([...$schemaNames, ...$dependencyNames]))
    : array_values(array_unique($schemaNames));
$expectedSchemaHashes = [];
$expectedDescriptorHashes = [];
foreach ($families as $family) {
    if (!is_array($family)) {
        continue;
    }
    if (is_string($family['source'] ?? null) && is_string($family['source_sha256'] ?? null)) {
        $expectedSchemaHashes[$family['source']] = $family['source_sha256'];
    }
    if (is_string($family['descriptor'] ?? null) && is_string($family['descriptor_sha256'] ?? null)) {
        $expectedDescriptorHashes[$family['descriptor']] = $family['descriptor_sha256'];
    }
}
$schemaFiles = [];
foreach ($schemaNames as $relative) {
    $source = null;
    foreach ($schemaRoots as $candidate) {
        $path = $candidate . DIRECTORY_SEPARATOR . str_replace('/', DIRECTORY_SEPARATOR, $relative);
        if (is_file($path)) {
            $source = $path;
            break;
        }
    }
    if ($source === null) {
        throw new RuntimeException('schema input missing: ' . $relative);
    }
    if (isset($expectedSchemaHashes[$relative]) && sha256File($source) !== $expectedSchemaHashes[$relative]) {
        throw new RuntimeException('schema hash mismatch: ' . $relative);
    }
    $schemaFiles[$relative] = $source;
}
foreach ($expectedDescriptorHashes as $relative => $expected) {
    $descriptor = null;
    foreach ($schemaRoots as $candidate) {
        $path = $candidate . DIRECTORY_SEPARATOR . str_replace('/', DIRECTORY_SEPARATOR, $relative);
        if (is_file($path)) {
            $descriptor = $path;
            break;
        }
    }
    if ($descriptor === null) {
        throw new RuntimeException('descriptor input missing: ' . $relative);
    }
    if (sha256File($descriptor) !== $expected) {
        throw new RuntimeException('descriptor hash mismatch: ' . $relative);
    }
}

$typePolicyMetadata = null;
if ($manifestPath !== null) {
    $typePolicySource = null;
    foreach ($schemaRoots as $candidate) {
        $path = $candidate . DIRECTORY_SEPARATOR . 'type-policy.json';
        if (is_file($path)) {
            $typePolicySource = $path;
            break;
        }
    }
    if ($typePolicySource === null) {
        throw new RuntimeException('Rust-owned type policy missing from schema root');
    }
    $typePolicyBytes = file_get_contents($typePolicySource);
    if ($typePolicyBytes === false) {
        throw new RuntimeException('unable to read Rust-owned type policy: ' . $typePolicySource);
    }
    $typePolicy = json_decode($typePolicyBytes, true, 512, JSON_THROW_ON_ERROR);
    if (($typePolicy['schema'] ?? null) !== 'acyclic.sdk.type-policy.v1') {
        throw new RuntimeException('unexpected Rust type policy schema');
    }
    $profile = null;
    foreach (($typePolicy['languages'] ?? []) as $entry) {
        if (is_array($entry) && ($entry['language'] ?? null) === 'php') {
            $profile = $entry;
            break;
        }
    }
    if (!is_array($profile) || !is_string($profile['nominal_types'] ?? null) || !is_string($profile['refinements'] ?? null) || !is_string($profile['unions'] ?? null)) {
        throw new RuntimeException('Rust type policy has no PHP profile');
    }
    $typePolicyDestination = $root . '/type-policy.json';
    if (file_put_contents($typePolicyDestination, $typePolicyBytes) === false) {
        throw new RuntimeException('unable to write Rust-owned type policy');
    }
    $typePolicyMetadata = [
        'path' => 'type-policy.json',
        'sha256' => hash('sha256', $typePolicyBytes),
        'schema' => $typePolicy['schema'],
        'language' => 'php',
        'profile' => $profile,
    ];
}

function executable(string $name): string
{
    $override = getenv(strtoupper(str_replace('-', '_', $name)));
    if ($override !== false && $override !== '') {
        if (PHP_OS_FAMILY === 'Windows' && !is_file($override)) {
            throw new RuntimeException($name . ' override does not point to an executable: ' . $override);
        }
        return $override;
    }
    $which = PHP_OS_FAMILY === 'Windows' ? 'where ' : 'command -v ';
    $result = shell_exec($which . escapeshellarg($name));
    $path = trim((string) $result);
    if ($path === '') {
        throw new RuntimeException($name . ' is required; install the pinned protobuf/gRPC toolchain');
    }
    return strtok($path, PHP_EOL) ?: $path;
}

function runCommand(array $arguments): void
{
    $command = implode(' ', array_map('escapeshellarg', $arguments));
    passthru($command, $status);
    if ($status !== 0) {
        throw new RuntimeException('generation failed with status ' . $status);
    }
}

$protoc = executable('protoc');
$plugin = executable('grpc_php_plugin');
$preserved = [
    // Rust-owned runtime and transport policy facades. Protobuf generation
    // may replace message/service files, but it must not erase the facade
    // layer that gives PHP the same automatic transport selection contract as
    // the other generated SDKs.
    'acyclic/runtime/uint64.php',
    'acyclic/runtime/generatedremotepolicy.php',
    'acyclic/runtime/remotepolicy.php',
    'acyclic/runtime/remoteclient.php',
    // The options descriptor is imported by generated files but is not
    // emitted by protoc for every target invocation. Preserve the Rust-owned
    // metadata class so clean package regeneration remains loadable.
    'gpbmetadata/validation/v1/options.php',
];
if (is_dir($output)) {
    $iterator = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($output, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST
    );
    foreach ($iterator as $path) {
        $relative = strtolower(str_replace('\\', '/', substr($path->getPathname(), strlen($output) + 1)));
        if ($path->isFile() && !in_array($relative, $preserved, true)) {
            unlink($path->getPathname());
        }
    }
}

$arguments = [
    $protoc,
    ...array_reduce($schemaRoots, static fn (array $carry, string $schemaRoot): array => [...$carry, '-I', $schemaRoot], []),
    '--php_out=' . $output,
    '--grpc_out=' . $output,
    '--plugin=protoc-gen-grpc=' . $plugin,
    ...$schemaNames,
];

chdir($root);
runCommand($arguments);

$rustFamilyGoldens = null;
if ($manifestPath !== null) {
    $fixtureSource = $schemaRoots[0] . DIRECTORY_SEPARATOR . 'rust-family-goldens.json';
    if (!is_file($fixtureSource)) {
        throw new RuntimeException('Rust-owned fixture missing: ' . $fixtureSource);
    }
    $fixtureBytes = file_get_contents($fixtureSource);
    if ($fixtureBytes === false) {
        throw new RuntimeException('unable to read Rust-owned fixture: ' . $fixtureSource);
    }
    $fixture = json_decode($fixtureBytes, true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($fixture) || count($fixture) !== 9) {
        throw new RuntimeException('Rust-owned fixture must contain nine family goldens');
    }
    $manifestHash = sha256File($manifestPath);
    foreach ($fixture as $entry) {
        if (!is_array($entry) || ($entry['authority_manifest_sha256'] ?? null) !== $manifestHash) {
            throw new RuntimeException('Rust-owned fixture is bound to a different authority manifest');
        }
    }
    $fixtureDestination = $root . '/tests/fixtures/rust-family-goldens.json';
    if (!is_dir(dirname($fixtureDestination)) && !mkdir(dirname($fixtureDestination), 0777, true) && !is_dir(dirname($fixtureDestination))) {
        throw new RuntimeException('unable to create fixture directory');
    }
    if (file_put_contents($fixtureDestination, $fixtureBytes) === false) {
        throw new RuntimeException('unable to write generated Rust-owned fixture');
    }
    $rustFamilyGoldens = [
        'path' => 'tests/fixtures/rust-family-goldens.json',
        'sha256' => hash('sha256', $fixtureBytes),
    ];
}

$generated = [];
$iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($output, FilesystemIterator::SKIP_DOTS));
foreach ($iterator as $path) {
    if ($path->isFile() && str_ends_with($path->getFilename(), '.php')) {
        $contents = file_get_contents($path->getPathname());
        if ($contents === false) {
            throw new RuntimeException('unable to read generated file: ' . $path->getPathname());
        }
        $qualified = '$var = \\Acyclic\\Runtime\\UInt64::validateGeneratedScalar($var);';
        $rewritten = str_replace('GPBUtil::checkUint64($var);', $qualified, $contents);
        if ($rewritten !== $contents && file_put_contents($path->getPathname(), $rewritten) === false) {
            throw new RuntimeException('unable to apply uint64 policy to generated file: ' . $path->getPathname());
        }
        $generated[] = str_replace('\\', '/', substr($path->getPathname(), strlen($root) + 1));
    }
}
sort($generated);
$provenance = [
    'generator' => $lock,
    'source_revision' => getenv('GIT_COMMIT') ?: 'unknown',
    'source_git_sha' => getenv('GIT_COMMIT') ?: 'unknown',
    'rust_model_digest' => getenv('ACYCLIC_RUST_MODEL_DIGEST') ?: 'unknown',
    'generator_lock_sha256' => sha256File($root . '/generator.lock.json'),
    'schema_inputs_sha256' => array_map('sha256File', $schemaFiles),
    'schema_root' => $schemaRootOption === null ? 'diagnostic repository proto roots' : str_replace('\\', '/', $schemaRoots[0]),
    'authority_manifest' => $manifestPath === null ? null : str_replace('\\', '/', $manifestPath),
    'authority_manifest_sha256' => $manifestPath === null ? null : sha256File($manifestPath),
    'authority_manifest_schema' => $authority['schema'] ?? null,
    'authority_source_revision' => $authority['source_revision'] ?? null,
    'authority_exporter' => $authority['exporter'] ?? null,
    'rust_family_goldens' => $rustFamilyGoldens,
    'type_policy' => $typePolicyMetadata,
    'generated_files' => $generated,
];
if (!preg_match('/^[0-9a-f]{40}$/i', $provenance['source_git_sha'])) {
    throw new RuntimeException('PHP provenance requires a 40-character Rust source Git SHA (GIT_COMMIT)');
}
if (!preg_match('/^[0-9a-f]{64}$/i', $provenance['rust_model_digest'])) {
    throw new RuntimeException('PHP provenance requires the 64-character Rust model digest (ACYCLIC_RUST_MODEL_DIGEST)');
}
if (($authority['source_revision'] ?? null) !== null && $authority['source_revision'] !== $provenance['rust_model_digest']) {
    throw new RuntimeException('PHP provenance model digest does not match the Rust authority manifest');
}
file_put_contents($output . '/provenance.json', json_encode($provenance, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR) . PHP_EOL);
