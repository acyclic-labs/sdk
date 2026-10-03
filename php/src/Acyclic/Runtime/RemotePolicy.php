<?php

// PROTOTYPE ONLY: handwritten adapter pending Rust generator emission.

declare(strict_types=1);

namespace Acyclic\Runtime;

use InvalidArgumentException;

/**
 * PROTOTYPE ONLY: handwritten adapter pending Rust generator emission.
 * Rust policy metadata and selection must be emitted into this package before
 * this helper can be treated as qualified generated output.
 */
final class RemotePolicy
{
    public const GRPC = 'grpc';
    public const GRPC_WEB = 'grpc_web';
    public const HTTP_JSON = 'http_json';

    /** @var array<string, array<string, list<array{kind:string, streaming:bool}>>> */
    private const OPTIONS = [
        'native' => [
            'actors' => [['kind' => self::GRPC, 'streaming' => false], ['kind' => self::HTTP_JSON, 'streaming' => false]],
            'workers' => [['kind' => self::GRPC, 'streaming' => false], ['kind' => self::HTTP_JSON, 'streaming' => false]],
            'objects' => [['kind' => self::GRPC, 'streaming' => true], ['kind' => self::HTTP_JSON, 'streaming' => true]],
            'stream' => [['kind' => self::GRPC, 'streaming' => true], ['kind' => self::HTTP_JSON, 'streaming' => true]],
            'inference' => [['kind' => self::GRPC, 'streaming' => true]],
            'machines' => [['kind' => self::GRPC, 'streaming' => true]],
            'filesystem' => [['kind' => self::GRPC, 'streaming' => true]],
            'harness' => [['kind' => self::GRPC, 'streaming' => true]],
        ],
        'browser' => [
            'actors' => [['kind' => self::HTTP_JSON, 'streaming' => false]],
            'workers' => [['kind' => self::HTTP_JSON, 'streaming' => false]],
            'objects' => [['kind' => self::HTTP_JSON, 'streaming' => true]],
            'stream' => [['kind' => self::HTTP_JSON, 'streaming' => true]],
            'inference' => [['kind' => self::HTTP_JSON, 'streaming' => true]],
            'machines' => [],
            'filesystem' => [['kind' => self::GRPC_WEB, 'streaming' => true]],
            'harness' => [],
        ],
    ];

    public static function select(string $family, bool $streaming = false, string $runtime = 'native', ?string $override = null): string
    {
        $options = self::OPTIONS[$runtime][$family] ?? throw new InvalidArgumentException("unknown transport family or runtime: {$family}/{$runtime}");
        $compatible = array_values(array_filter($options, static fn (array $option): bool => !$streaming || $option['streaming']));
        if ($override !== null) {
            foreach ($compatible as $option) {
                if ($option['kind'] === $override) {
                    return $override;
                }
            }
            throw new InvalidArgumentException("unsupported transport override {$override} for {$family}/{$runtime}");
        }
        if ($compatible === []) {
            throw new InvalidArgumentException("no compatible transport for {$family}/{$runtime}");
        }
        return $compatible[0]['kind'];
    }

    public static function validateBearer(string $token): string
    {
        if (trim($token) === '' || preg_match('/[\r\n]/', $token) === 1) {
            throw new InvalidArgumentException('invalid bearer credential');
        }
        return $token;
    }
}
