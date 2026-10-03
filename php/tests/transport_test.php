<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

$generated = dirname(__DIR__) . '/src';
$required = [
    $generated . '/Acyclic/Actors/V1/ActorsServiceClient.php',
    $generated . '/Acyclic/Stream/V2/StreamServiceClient.php',
];
foreach ($required as $path) {
    if (!is_file($path)) {
        throw new RuntimeException('generated transport is missing: ' . $path . '; run composer generate');
    }
}

$max = \Acyclic\Runtime\UInt64::fromString('18446744073709551615');
if ($max->toString() !== '18446744073709551615'
    || json_encode($max, JSON_THROW_ON_ERROR) !== '"18446744073709551615"'
    || $max->toWireVarint() !== str_repeat("\xff", 9) . "\x01"
    || \Acyclic\Runtime\UInt64::fromWireVarint($max->toWireVarint())->toString() !== $max->toString()) {
    throw new RuntimeException('uint64 max was not preserved by the exact decimal wrapper');
}
$limits = new \Acyclic\Actors\V1\ActorLimits();
$limits->setMemoryBytes('9223372036854775807');
if ((string) $limits->getMemoryBytes() !== '9223372036854775807') {
    throw new RuntimeException('signed-range uint64 was not preserved by generated protobuf accessors');
}
try {
    $limits->setMemoryBytes($max->toString());
    throw new RuntimeException('generated uint64 accessor silently clamped above PHP_INT_MAX');
} catch (InvalidArgumentException $expected) {
    if (!str_contains($expected->getMessage(), 'Acyclic\\Runtime\\UInt64')) {
        throw new RuntimeException('generated uint64 overflow did not identify the exact wrapper path');
    }
}
$observation = new \Acyclic\Actors\V1\ActorObservation();
$observation->setCodeSha256("\x00\xff");
if ($observation->getCodeSha256() !== "\x00\xff") {
    throw new RuntimeException('binary bytes were not preserved by generated protobuf accessors');
}
$append = new \Acyclic\Stream\V2\AppendRequest();
if ($append->hasIfTail()) {
    throw new RuntimeException('optional uint64 unexpectedly has presence before assignment');
}
$append->setIfTail(9007199254740992);
if (!$append->hasIfTail() || (string) $append->getIfTail() !== '9007199254740992') {
    throw new RuntimeException('optional uint64 presence or value was not preserved');
}
foreach (['read', 'follow', 'children'] as $method) {
    if (!method_exists(\Acyclic\Stream\V2\StreamServiceClient::class, $method)) {
        throw new RuntimeException('generated Stream client is missing server stream method ' . $method);
    }
}

// Generated protobuf scalar accessors are used only through PHP_INT_MAX. The
// exact decimal wrapper above is required for values in the unsigned range
// above that boundary; JSON emits those values as strings by policy.
if (PHP_INT_SIZE < 8) {
    throw new RuntimeException('the transport package requires 64-bit PHP for uint64 conformance');
}
$vectors = [0, 4294967295, 9007199254740991, 9007199254740992, 9223372036854775807];
foreach ($vectors as $vector) {
    if ((int) (string) $vector !== $vector) {
        throw new RuntimeException('uint64 smoke vector was not preserved: ' . $vector);
    }
}

echo "transport package smoke checks passed\n";
