<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

if (!extension_loaded('protobuf')) {
    throw new RuntimeException(
        'the Rust generated-message fixtures require the official protobuf extension'
    );
}

$fixtures = json_decode(
    (string) file_get_contents(__DIR__ . '/fixtures/rust-family-goldens.json'),
    true,
    512,
    JSON_THROW_ON_ERROR
);
$provenance = json_decode(
    (string) file_get_contents(dirname(__DIR__) . '/src/provenance.json'),
    true,
    512,
    JSON_THROW_ON_ERROR
);
if (count($fixtures) !== 9) {
    throw new RuntimeException('expected one Rust golden per authority family');
}
$seenFamilies = [];
foreach ($fixtures as $fixture) {
    if (($fixture['authority_manifest_sha256'] ?? null) !== ($provenance['authority_manifest_sha256'] ?? null)) {
        throw new RuntimeException('Rust family fixture is bound to a different authority manifest');
    }
    $parts = explode('.', (string) $fixture['message']);
    $shortName = array_pop($parts);
    $namespace = implode('\\', array_map(static fn (string $part): string => ucfirst($part), $parts));
    $class = '\\' . $namespace . '\\' . $shortName;
    if (!class_exists($class)) {
        throw new RuntimeException("generated class missing for {$fixture['message']}");
    }
    $wire = hex2bin((string) $fixture['wire_hex']);
    if ($wire === false) {
        throw new RuntimeException("invalid golden wire for {$fixture['family']}");
    }
    $message = new $class();
    $message->mergeFromString($wire);
    if ($message->serializeToString() !== $wire) {
        throw new RuntimeException("native generated message changed Rust {$fixture['family']} golden wire bytes");
    }
    if ($message->serializeToJsonString() !== $fixture['json']) {
        throw new RuntimeException("native generated message changed Rust {$fixture['family']} golden JSON");
    }
    $decoded = json_decode($message->serializeToJsonString(), true, 512, JSON_THROW_ON_ERROR);
    if (($fixture['kind'] ?? null) === 'uint64') {
        $exact = \Acyclic\Runtime\UInt64::fromString((string) ($decoded[$fixture['field']] ?? ''));
        if ($exact->toString() !== $fixture['value']) {
            throw new RuntimeException("generated {$fixture['family']} JSON did not preserve exact UInt64");
        }
    } elseif (($decoded[$fixture['field']] ?? null) !== $fixture['value']) {
        throw new RuntimeException("generated {$fixture['family']} scalar changed");
    }
    $seenFamilies[] = $fixture['family'];
}
if (count(array_unique($seenFamilies)) !== 9) {
    throw new RuntimeException('Rust family fixture set did not cover all nine families');
}

echo "Rust all-family generated-message goldens passed (9/9; native protobuf)\n";
