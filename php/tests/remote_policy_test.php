<?php

declare(strict_types=1);

$autoload = getenv('ACYCLIC_VENDOR_AUTOLOAD') ?: dirname(__DIR__) . '/vendor/autoload.php';
require $autoload;
if (getenv('ACYCLIC_REMOTE_POLICY_SOURCE') !== false) {
    require getenv('ACYCLIC_REMOTE_POLICY_SOURCE') . '/RemotePolicy.php';
    require getenv('ACYCLIC_REMOTE_POLICY_SOURCE') . '/RemoteClient.php';
}

use Acyclic\Runtime\RemoteClient;
use Acyclic\Runtime\RemotePolicy;

$calls = [];
$client = new RemoteClient('stream', static function (string $operation, array $request, string $transport) use (&$calls): string {
    $calls[] = [$operation, $request, $transport];
    return 'ok';
});
 $expectedNative = extension_loaded('grpc') ? RemotePolicy::GRPC : RemotePolicy::HTTP_JSON;
if ($client->transport() !== $expectedNative || $client->call('append', ['path' => 'events']) !== 'ok'
    || $calls !== [['append', ['path' => 'events'], $expectedNative]]) {
    throw new RuntimeException('native gRPC facade selection or invocation failed');
}
if ($client->runtime() !== 'native' || RemotePolicy::resolveRuntime() !== 'native') {
    throw new RuntimeException('automatic native runtime resolution failed');
}
if (class_exists('Acyclic\\TypePolicy\\Path')) {
    $typed = new RemoteClient(
        'stream',
        static function (string $operation, array $request, string $transport) use (&$calls): string {
            $calls[] = [$operation, $request, $transport];
            return 'typed-ok';
        },
        'native',
        false,
        null,
        null,
        true,
        ['grpc' => false, 'http_json' => true],
        ['grpc' => false, 'http_json' => true],
    );
    if ($typed->call('append', ['path' => \Acyclic\TypePolicy\Path::from('events')]) !== 'typed-ok') {
        throw new RuntimeException('Rust-owned PHP value object was not accepted by the public facade');
    }
    try {
        $typed->call('append', ['path' => '']);
        throw new RuntimeException('invalid Rust-owned PHP value was accepted by the public facade');
    } catch (InvalidArgumentException $expected) {
    }
}
$browser = RemotePolicy::select('stream', true, 'browser');
if ($browser !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('embedded browser runtime did not select HTTP JSON');
}
$http = new RemoteClient('stream', static fn (...$args): null => null, 'native', true, RemotePolicy::HTTP_JSON);
if ($http->transport() !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('streaming HTTP JSON override was not selected');
}
$capability = new RemoteClient(
    'actors',
    static fn (...$args): null => null,
    'native',
    false,
    null,
    null,
    true,
    ['grpc' => false, 'http_json' => true],
    ['grpc' => false, 'http_json' => true],
);
if ($capability->transport() !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('installed and endpoint capabilities were not forwarded');
}
$previousEndpointTransports = getenv('ACYCLIC_ENDPOINT_TRANSPORTS');
putenv('ACYCLIC_ENDPOINT_TRANSPORTS=http_json');
$metadata = new RemoteClient('actors', static fn (...$args): null => null);
if ($metadata->transport() !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('endpoint metadata was not consumed automatically');
}
$https = new RemoteClient('actors', static fn (...$args): null => null, 'native', false, null, ' token ', true, null, 'https://api.example');
if ($https->transport() !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('HTTPS endpoint metadata did not select HTTP JSON');
}
if ($previousEndpointTransports === false) {
    putenv('ACYCLIC_ENDPOINT_TRANSPORTS');
} else {
    putenv('ACYCLIC_ENDPOINT_TRANSPORTS=' . $previousEndpointTransports);
}
try {
    new RemoteClient(
        'actors',
        static fn (...$args): null => null,
        'native',
        false,
        null,
        null,
        true,
        ['grpc' => true, 'http_json' => true],
        ['grpc' => false, 'http_json' => false],
    );
    throw new RuntimeException('endpoint with no compatible transport was accepted');
} catch (InvalidArgumentException $expected) {
}
try {
    new RemoteClient('actors', static fn (...$args): null => null, 'native', true, RemotePolicy::HTTP_JSON);
    throw new RuntimeException('unary-only HTTP override was accepted for streaming');
} catch (InvalidArgumentException $expected) {
}
foreach (['', "  ", "token\r\nInjected: yes"] as $invalid) {
    try {
        RemotePolicy::validateBearer($invalid);
        throw new RuntimeException('invalid bearer credential was accepted');
    } catch (InvalidArgumentException $expected) {
    }
}
echo "remote policy checks passed\n";
