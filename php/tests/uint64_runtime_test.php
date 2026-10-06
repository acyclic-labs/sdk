<?php

declare(strict_types=1);

require dirname(__DIR__) . '/src/Acyclic/Runtime/UInt64.php';

use Acyclic\Runtime\UInt64;

/** @param callable(): mixed $attempt */
function assertThrows(callable $attempt, string $name): void
{
    try {
        $attempt();
    } catch (InvalidArgumentException) {
        return;
    }
    throw new RuntimeException($name . ' was accepted');
}

$values = [
    '0',
    '1',
    '127',
    '128',
    '4294967295',
    '9007199254740991',
    '9007199254740992',
    '9223372036854775807',
    '9223372036854775808',
    '18446744073709551615',
];
foreach ($values as $value) {
    $exact = UInt64::fromString($value);
    if ($exact->toString() !== $value
        || $exact->toWire() !== $value
        || (string) $exact !== $value
        || json_encode($exact, JSON_THROW_ON_ERROR) !== json_encode($value, JSON_THROW_ON_ERROR)
        || UInt64::fromWireVarint($exact->toWireVarint())->toString() !== $value
        || UInt64::fromWireScalar($value)->toString() !== $value) {
        throw new RuntimeException('uint64 exact round trip failed for ' . $value);
    }
}

if (PHP_INT_SIZE >= 8
    && UInt64::fromWireScalar(PHP_INT_MAX)->toString() !== (string) PHP_INT_MAX) {
    throw new RuntimeException('uint64 PHP integer scalar was not preserved');
}

foreach (['', '-1', '+1', '01', ' 1', '1 ', '1.0', '18446744073709551616'] as $value) {
    assertThrows(static fn(): UInt64 => UInt64::fromString($value), 'invalid decimal ' . var_export($value, true));
}
assertThrows(static fn(): UInt64 => UInt64::fromInt(-1), 'negative integer');
foreach (['', "\x80", "\x00\x00", str_repeat("\x80", 10), str_repeat("\x80", 9) . "\x02"] as $wire) {
    assertThrows(static fn(): UInt64 => UInt64::fromWireVarint($wire), 'invalid varint ' . bin2hex($wire));
}
assertThrows(static fn(): UInt64 => UInt64::fromWireScalar(-1), 'negative wire scalar');

echo "pure PHP UInt64 edge-case checks passed\n";
