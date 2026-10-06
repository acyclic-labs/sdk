<?php

// Deliberately omit strict_types: generated constructors must enforce the
// Rust-owned type policy even when a consumer's calling file uses PHP's weak
// scalar coercion rules.

require dirname(__DIR__) . '/src/Acyclic/Generated/RustTyped.php';

use Acyclic\Generated\RustAcyclicStreamV2AppendResponseOutcomeChoiceUnknown;
use Acyclic\Generated\RustIdempotencyKeyText;
use Acyclic\Generated\RustStreamPageLimit;

/** @param callable(): mixed $attempt */
function assertInvalidArgument(callable $attempt, string $name): void
{
    try {
        $attempt();
    } catch (InvalidArgumentException) {
        return;
    }
    throw new RuntimeException($name . ' accepted a weakly coerced value');
}

assertInvalidArgument(
    static fn(): RustStreamPageLimit => new RustStreamPageLimit('4'),
    'integer semantic wrapper',
);
assertInvalidArgument(
    static fn(): RustIdempotencyKeyText => new RustIdempotencyKeyText(7),
    'string semantic wrapper',
);
assertInvalidArgument(
    static fn(): RustAcyclicStreamV2AppendResponseOutcomeChoiceUnknown
        => new RustAcyclicStreamV2AppendResponseOutcomeChoiceUnknown('7', "raw"),
    'unknown oneof tag',
);

$unknown = new RustAcyclicStreamV2AppendResponseOutcomeChoiceUnknown(7, "raw");
if ($unknown->unknownTag !== 7 || $unknown->payload !== "raw") {
    throw new RuntimeException('valid unknown oneof payload was not preserved');
}

echo "portable typed PHP weak-caller checks passed\n";
