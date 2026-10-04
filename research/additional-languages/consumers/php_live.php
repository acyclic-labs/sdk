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

$clients = [];
$results = [];
foreach ($manifest['methods'] as $entry) {
    $result = array_intersect_key($entry, array_flip([
        'family', 'package', 'service', 'method', 'path', 'request_type', 'response_type',
        'client_streaming', 'server_streaming'
    ]));
    $typed = $entry['typed_request'] ?? [];
    $hex = $typed['serialized_hex'] ?? null;
    if (!is_string($hex) || $hex === '') {
        $result['status'] = 'pending_missing_typed_request';
        $result['request_sha256'] = null;
        $results[] = $result;
        continue;
    }
    try {
        $clientClass = phpClass($entry['package'] . '.' . $entry['service'] . 'Client');
        $requestClass = phpClass($entry['request_type']);
        $cacheKey = $clientClass;
        $clients[$cacheKey] ??= new $clientClass($endpoint, ['credentials' => Grpc\ChannelCredentials::createInsecure()]);
        $request = new $requestClass();
        $request->mergeFromString(hex2bin($hex));
        $callOptions = ['timeout' => $timeoutMs * 1000];
        $call = $clients[$cacheKey]->{$entry['method']}($request, [], $callOptions);
        $frames = [];
        if ($entry['server_streaming']) {
            foreach ($call->responses() as $frame) $frames[] = responseValue($frame);
            $result['response_frames'] = $frames;
            $result['status_info'] = $call->getStatus();
        } else {
            [$response, $status] = $call->wait();
            $result['status_info'] = $status;
            if ($response !== null) $result['response'] = responseValue($response);
            if (($status->code ?? 0) === Grpc\STATUS_OK) $result['status'] = 'transport_success_pending_semantics';
            else $result['status'] = 'error';
        }
        $result['request_sha256'] = hash('sha256', hex2bin($hex));
    } catch (Throwable $e) {
        $result['status'] = 'error';
        $result['error'] = ['class' => get_class($e), 'message' => $e->getMessage()];
        $result['request_sha256'] = hash('sha256', hex2bin($hex));
    }
    $results[] = $result;
}

$receipt = [
    'schema' => 'acyclic.sdk.rpd.php-live-receipt.v1',
    'authority' => $authority,
    'endpoint' => $endpoint,
    'package_root' => realpath($packageRoot),
    'method_count' => count($results),
    'passed' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'semantic_passed')),
    'transport_succeeded' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'transport_success_pending_semantics')),
    'pending' => count(array_filter($results, static fn(array $r): bool => ($r['status'] ?? '') === 'pending_missing_typed_request')),
    'methods' => $results,
];
file_put_contents($output, json_encode($receipt, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL);
echo json_encode(['method_count' => count($results), 'passed' => $receipt['passed'], 'pending' => $receipt['pending']) . PHP_EOL;
