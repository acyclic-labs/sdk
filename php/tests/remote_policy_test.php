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
if ($client->transport() !== RemotePolicy::GRPC || $client->call('append', ['path' => 'events']) !== 'ok'
    || $calls !== [['append', ['path' => 'events'], RemotePolicy::GRPC]]) {
    throw new RuntimeException('native gRPC facade selection or invocation failed');
}
$http = new RemoteClient('stream', static fn (...$args): null => null, 'native', true, RemotePolicy::HTTP_JSON);
if ($http->transport() !== RemotePolicy::HTTP_JSON) {
    throw new RuntimeException('streaming HTTP JSON override was not selected');
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
