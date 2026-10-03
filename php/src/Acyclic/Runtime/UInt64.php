<?php

declare(strict_types=1);

namespace Acyclic\Runtime;

use InvalidArgumentException;
use JsonSerializable;
use Stringable;

/**
 * Exact unsigned 64-bit value for PHP boundaries.
 *
 * The pure-PHP google/protobuf runtime narrows uint64 setters to PHP_INT_MAX
 * on 64-bit builds. Keep values above that boundary as decimal strings and
 * encode/decode their protobuf varints without floating-point conversion.
 */
final class UInt64 implements JsonSerializable, Stringable
{
    private const MAX = '18446744073709551615';

    private function __construct(private readonly string $decimal)
    {
    }

    public static function fromString(string $value): self
    {
        if (!preg_match('/^(?:0|[1-9][0-9]*)$/', $value)) {
            throw new InvalidArgumentException('uint64 must be a non-negative decimal string');
        }
        if (self::compare($value, self::MAX) > 0) {
            throw new InvalidArgumentException('uint64 is greater than 18446744073709551615');
        }
        return new self($value);
    }

    public static function fromInt(int $value): self
    {
        if ($value < 0) {
            throw new InvalidArgumentException('uint64 cannot be negative');
        }
        return new self((string) $value);
    }

    /**
     * Validate a generated protobuf scalar without allowing PHP integer
     * conversion to silently clamp an unsigned value above PHP_INT_MAX.
     *
     * Generated accessors remain compatible with the official protobuf
     * runtime for the signed 64-bit range. Callers that need the complete
     * uint64 range use this class directly for exact JSON and wire output.
     */
    public static function validateGeneratedScalar(int|string $value): int|string
    {
        if (is_int($value)) {
            if ($value < 0) {
                throw new InvalidArgumentException('uint64 cannot be negative');
            }
            return $value;
        }

        $exact = self::fromString($value)->toString();
        if (self::compare($exact, (string) PHP_INT_MAX) > 0) {
            throw new InvalidArgumentException(
                'uint64 above PHP_INT_MAX requires Acyclic\\Runtime\\UInt64 for exact representation'
            );
        }
        return (int) $exact;
    }

    public static function fromWireVarint(string $wire): self
    {
        if ($wire === '') {
            throw new InvalidArgumentException('uint64 varint cannot be empty');
        }
        $value = '0';
        $multiplier = '1';
        $length = strlen($wire);
        for ($index = 0; $index < $length; $index++) {
            $byte = ord($wire[$index]);
            if ($index === 9 && ($byte & 0x7e) !== 0) {
                throw new InvalidArgumentException('uint64 varint exceeds 64 bits');
            }
            $value = self::add($value, self::multiplySmall($multiplier, $byte & 0x7f));
            $multiplier = self::multiplySmall($multiplier, 128);
            if (($byte & 0x80) === 0) {
                if ($index !== $length - 1) {
                    throw new InvalidArgumentException('uint64 varint has trailing bytes');
                }
                return self::fromString($value);
            }
            if ($index === 9) {
                throw new InvalidArgumentException('uint64 varint exceeds 64 bits');
            }
        }
        throw new InvalidArgumentException('uint64 varint is unterminated');
    }

    public function toString(): string
    {
        return $this->decimal;
    }

    public function __toString(): string
    {
        return $this->decimal;
    }

    public function jsonSerialize(): string
    {
        return $this->decimal;
    }

    /** Return the protobuf wire value for a uint64 field without clamping. */
    public function toWireVarint(): string
    {
        $value = $this->decimal;
        $wire = '';
        while ($value !== '0') {
            [$value, $remainder] = self::divideSmall($value, 128);
            $wire .= chr($remainder | ($value !== '0' ? 0x80 : 0));
        }
        return $wire === '' ? "\x00" : $wire;
    }

    private static function compare(string $left, string $right): int
    {
        $left = ltrim($left, '0') ?: '0';
        $right = ltrim($right, '0') ?: '0';
        return strlen($left) <=> strlen($right) ?: strcmp($left, $right);
    }

    private static function add(string $left, string $right): string
    {
        $index = strlen($left) - 1;
        $other = strlen($right) - 1;
        $carry = 0;
        $result = '';
        while ($index >= 0 || $other >= 0 || $carry !== 0) {
            $sum = $carry + ($index >= 0 ? (int) $left[$index--] : 0)
                + ($other >= 0 ? (int) $right[$other--] : 0);
            $result = (string) ($sum % 10) . $result;
            $carry = intdiv($sum, 10);
        }
        return ltrim($result, '0') ?: '0';
    }

    private static function multiplySmall(string $value, int $factor): string
    {
        $carry = 0;
        $result = '';
        for ($index = strlen($value) - 1; $index >= 0; $index--) {
            $product = ((int) $value[$index] * $factor) + $carry;
            $result = (string) ($product % 10) . $result;
            $carry = intdiv($product, 10);
        }
        while ($carry > 0) {
            $result = (string) ($carry % 10) . $result;
            $carry = intdiv($carry, 10);
        }
        return ltrim($result, '0') ?: '0';
    }

    /** @return array{string, int} */
    private static function divideSmall(string $value, int $divisor): array
    {
        $quotient = '';
        $carry = 0;
        for ($index = 0; $index < strlen($value); $index++) {
            $current = ($carry * 10) + (int) $value[$index];
            $digit = intdiv($current, $divisor);
            if ($quotient !== '' || $digit !== 0) {
                $quotient .= (string) $digit;
            }
            $carry = $current % $divisor;
        }
        return [$quotient === '' ? '0' : $quotient, $carry];
    }
}
