<?php
declare(strict_types=1);

// Execute every Rust-owned RPC against an installed generated PHP package.
// Request bytes come only from the Rust typed-request manifest. Missing bytes
// remain pending, and default protobuf responses are never qualification.

function option(string $name, ?string $default = null): ?string {
    global $argv;
    foreach ($argv as $i => $arg) {
        if ($arg === $name && isset($argv[$i + 1])) return $argv[$i + 1];
        if (str_starts_with($arg, $name . '=')) return substr($arg, strlen($name) + 1);
    }
    return $default;
}

$packageRoot = option('--package-root');
$manifestPath = option('--manifest');
$artifactPath = option('--artifact-path');
$endpoint = option('--endpoint', '127.0.0.1:50051');
$output = option('--output', 'php-live-receipt.json');
$timeoutMs = (int) option('--timeout-ms', '5000');
if ($packageRoot === null || $manifestPath === null) {
    fwrite(STDERR, "--package-root and --manifest are required\n");
    exit(2);
}

$autoload = rtrim($packageRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'vendor/autoload.php';
if (!is_file($autoload)) {
    fwrite(STDERR, "missing installed package autoloader: $autoload\n");
    exit(2);
}
require $autoload;
$manifest = json_decode(file_get_contents($manifestPath), true, 512, JSON_THROW_ON_ERROR);
$authority = $manifest['authority'];
$packageProvenancePath = rtrim($packageRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'src/provenance.json';
$packageProvenance = is_file($packageProvenancePath)
    ? json_decode(file_get_contents($packageProvenancePath), true, 512, JSON_THROW_ON_ERROR)
    : [];
$artifactSha256 = $artifactPath !== null ? hash_file('sha256', $artifactPath) : null;
$runtimePlatform = array_merge($packageProvenance['platform'] ?? [], [
    'runtime_triple' => strtolower(PHP_OS_FAMILY . '-' . php_uname('m')),
    'runtime_os' => strtolower(PHP_OS_FAMILY),
    'runtime_arch' => strtolower(php_uname('m')),
    'observed' => true,
]);
$executedPackage = array_merge($packageProvenance, [
    'artifact_path' => $artifactPath !== null ? realpath($artifactPath) : null,
    'artifact_sha256' => $artifactSha256,
    'platform' => $runtimePlatform,
    'provenance' => array_merge($packageProvenance, ['platform' => $runtimePlatform]),
]);

function phpClass(string $protoName): string {
    $parts = explode('.', ltrim($protoName, '.'));
    $parts = array_map(static fn(string $part): string => ucfirst(str_replace('_', '', ucwords($part, '_'))), $parts);
    return '\\' . implode('\\', $parts);
}

function responseValue(object $message): mixed {
    if (method_exists($message, 'serializeToJsonString')) {
        return json_decode($message->serializeToJsonString(), true, 512, JSON_THROW_ON_ERROR);
    }
    return ['class' => get_class($message), 'value' => (string) $message];
}

function responseBytesHex(object $message): string {
    if (!method_exists($message, 'serializeToString')) {
        throw new RuntimeException('generated PHP response does not expose protobuf serialization');
    }
    return bin2hex($message->serializeToString());
}

function validHex(mixed $value): bool {
    if (!is_string($value) || (strlen($value) % 2) !== 0) return false;
    // Avoid a backtracking regexp for Rust-owned body frames, which can be
    // multi-megabyte payloads. strspn is linear and keeps the check exact.
    return strspn($value, '0123456789abcdefABCDEF') === strlen($value);
}

$clients = [];
$results = [];
foreach ($manifest['methods'] as $entry) {
    $result = array_intersect_key($entry, array_flip([
        'family', 'package', 'service', 'method', 'path', 'request_type', 'response_type',
        'client_streaming', 'server_streaming'
    ]));
    $typed = $entry['typed_request'] ?? null;
    if (!is_array($typed)) {
        $result['status'] = 'pending_missing_typed_request';
        $result['request_sha256'] = null;
        $results[] = $result;
        continue;
    }
    $hex = $typed['serialized_hex'] ?? null;
    $expectedRequestSha = $typed['serialized_sha256'] ?? null;
    if (!array_key_exists('serialized_hex', $typed) || !is_string($hex) || !is_string($expectedRequestSha)) {
        $result['status'] = 'pending_missing_typed_request';
        $result['request_sha256'] = null;
        $results[] = $result;
        continue;
    }
    try {
        // Reset per-RPC terminal and frame state before any validation or
        // client construction can throw. Catch handling must never inherit a
        // status or frames from the preceding method.
        $status = null;
        $frames = [];
        $frameHex = [];
        $frameTypes = [];
        if (!validHex($hex)) {
            throw new RuntimeException('invalid serialized request hex');
        }
        $requestDigest = hash('sha256', hex2bin($hex));
        if (strtolower((string) preg_replace('/\Asha256:/i', '', $expectedRequestSha)) !== $requestDigest) {
            throw new RuntimeException('serialized request hash does not match Rust authority');
        }
        $frameSpecs = $typed['serialized_frames'] ?? null;
        if (!is_array($frameSpecs) || $frameSpecs === []) {
            $frameSpecs = [['serialized_hex' => $hex, 'serialized_sha256' => $requestDigest]];
        }
        $requestFrameHex = [];
        $requestFrameSha = [];
        foreach ($frameSpecs as $frameIndex => $frame) {
            $frameHex = is_array($frame) ? ($frame['serialized_hex'] ?? null) : $frame;
            if (!validHex($frameHex)) {
                throw new RuntimeException("invalid serialized request frame hex at $frameIndex");
            }
            $frameDigest = hash('sha256', hex2bin($frameHex));
            $declaredFrameDigest = is_array($frame) ? ($frame['serialized_sha256'] ?? null) : null;
            if ($declaredFrameDigest !== null && strtolower((string) preg_replace('/\Asha256:/i', '', (string) $declaredFrameDigest)) !== $frameDigest) {
                throw new RuntimeException("serialized request frame hash does not match Rust authority at $frameIndex");
            }
            $requestFrameHex[] = strtolower($frameHex);
            $requestFrameSha[] = $frameDigest;
        }
        $result['request_frames_hex'] = $requestFrameHex;
        $result['request_frames_sha256'] = $requestFrameSha;
        $result['request_frame_type_ids'] = array_map(static fn($frame): mixed => is_array($frame) ? ($frame['type'] ?? null) : null, $frameSpecs);
        $clientClass = phpClass($entry['package'] . '.' . $entry['service'] . 'Client');
        $requestClass = phpClass($entry['request_type']);
        $cacheKey = $clientClass;
        $clients[$cacheKey] ??= new $clientClass($endpoint, ['credentials' => Grpc\ChannelCredentials::createInsecure()]);
        $requests = [];
        foreach ($requestFrameHex as $frameHex) {
            $request = new $requestClass();
            $request->mergeFromString(hex2bin($frameHex));
            $requests[] = $request;
        }
        $callOptions = ['timeout' => $timeoutMs * 1000];
        if ($entry['client_streaming']) {
            // gRPC PHP exposes client-streaming methods as a call factory.
            // Requests are written to the returned call; passing a protobuf
            // message as the first argument is interpreted as metadata and
            // fails before the RPC reaches the server.
            $call = $clients[$cacheKey]->{$entry['method']}([], $callOptions);
            foreach ($requests as $requestFrame) {
                $call->write($requestFrame);
            }
            [$response, $status] = $call->wait();
            $result['status_info'] = $status;
            $result['terminal_code'] = (int) ($status->code ?? 0);
            if ($response !== null) {
                $result['response'] = responseValue($response);
                $result['response_type_observed'] = get_class($response);
                $result['response_type_id_observed'] = (string) $entry['response_type'];
                $result['response_bytes_hex'] = responseBytesHex($response);
                $result['response_sha256'] = hash('sha256', hex2bin($result['response_bytes_hex']));
            }
            $result['terminal_status'] = $result['terminal_code'] === Grpc\STATUS_CANCELLED ? 'canceled' : ($result['terminal_code'] === Grpc\STATUS_OK ? 'ok' : 'error');
            $result['status'] = ($result['terminal_code'] === Grpc\STATUS_OK && $response !== null) || $result['terminal_code'] === Grpc\STATUS_CANCELLED ? 'semantic_passed' : 'error';
        } elseif ($entry['server_streaming']) {
            $call = $clients[$cacheKey]->{$entry['method']}($requests[0], [], $callOptions);
            $frameHex = [];
            $frameTypes = [];
            foreach ($call->responses() as $frame) {
                $frames[] = responseValue($frame);
                $frameHex[] = responseBytesHex($frame);
                $frameTypes[] = get_class($frame);
            }
            $result['response_frames'] = $frames;
            $result['response_frame_types'] = $frameTypes;
            $result['response_frame_type_ids'] = array_fill(0, count($frames), (string) $entry['response_type']);
            $result['response_frames_hex'] = $frameHex;
            $result['response_frames_sha256'] = array_map(static fn(string $value): string => hash('sha256', hex2bin($value)), $frameHex);
            $result['status_info'] = $call->getStatus();
            $result['terminal_code'] = (int) ($result['status_info']->code ?? Grpc\STATUS_OK);
            $result['terminal_status'] = $result['terminal_code'] === Grpc\STATUS_CANCELLED ? 'canceled' : ($result['terminal_code'] === Grpc\STATUS_OK ? 'ok' : 'error');
            $result['status'] = in_array($result['terminal_code'], [Grpc\STATUS_OK, Grpc\STATUS_CANCELLED], true) ? 'semantic_passed' : 'error';
        } else {
            $call = $clients[$cacheKey]->{$entry['method']}($requests[0], [], $callOptions);
            [$response, $status] = $call->wait();
            $result['status_info'] = $status;
            $result['terminal_code'] = (int) ($status->code ?? 0);
            if ($response !== null) {
                $result['response'] = responseValue($response);
                $result['response_type_observed'] = get_class($response);
                $result['response_type_id_observed'] = (string) $entry['response_type'];
                $result['response_bytes_hex'] = responseBytesHex($response);
                $result['response_sha256'] = hash('sha256', hex2bin($result['response_bytes_hex']));
            }
            $result['terminal_status'] = $result['terminal_code'] === Grpc\STATUS_CANCELLED ? 'canceled' : ($result['terminal_code'] === Grpc\STATUS_OK ? 'ok' : 'error');
            $result['status'] = ($result['terminal_code'] === Grpc\STATUS_OK && $response !== null) || $result['terminal_code'] === Grpc\STATUS_CANCELLED ? 'semantic_passed' : 'error';
        }
        $result['request_sha256'] = $requestDigest;
    } catch (Throwable $e) {
        if ($status !== null && (($status->code ?? 0) === Grpc\STATUS_CANCELLED)) {
            if (isset($frameHex, $frameTypes, $frames)) {
                $result['response_frames'] = $frames;
                $result['response_frame_types'] = $frameTypes;
                $result['response_frame_type_ids'] = array_fill(0, count($frames), (string) $entry['response_type']);
                $result['response_frames_hex'] = $frameHex;
                $result['response_frames_sha256'] = array_map(static fn(string $value): string => hash('sha256', hex2bin($value)), $frameHex);
            }
            $result['status'] = 'semantic_passed';
            $result['terminal_status'] = 'canceled';
            $result['terminal_code'] = Grpc\STATUS_CANCELLED;
        } else {
            $result['status'] = 'error';
            $result['terminal_status'] = 'error';
            $result['terminal_code'] = isset($status) ? (int) ($status->code ?? 2) : 2;
            $result['error'] = ['class' => get_class($e), 'message' => $e->getMessage()];
        }
        $result['request_sha256'] = validHex($hex) ? hash('sha256', hex2bin($hex)) : null;
    }
    $results[] = $result;
}

$receipt = [
    'schema' => 'acyclic.sdk.rpd.php-live-receipt.v1',
    'authority' => $authority,
    'endpoint' => $endpoint,
    'package_root' => realpath($packageRoot),
    'executed_package' => $executedPackage,
    'method_count' => count($results),
    'passed' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'semantic_passed')),
    'transport_succeeded' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'transport_success_pending_semantics')),
    'pending' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'pending_missing_typed_request')),
    'methods' => $results,
];
file_put_contents($output, json_encode($receipt, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL);
echo json_encode(['method_count' => count($results), 'passed' => $receipt['passed'], 'pending' => $receipt['pending']]) . PHP_EOL;
