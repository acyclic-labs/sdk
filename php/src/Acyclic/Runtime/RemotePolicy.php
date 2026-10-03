<?php

declare(strict_types=1);

namespace Acyclic\Runtime;

use InvalidArgumentException;

require_once __DIR__ . '/GeneratedRemotePolicy.php';

/** Compatibility facade over the Rust-emitted transport policy. */
final class RemotePolicy
{
    public const SOURCE_BINDING = GeneratedRemotePolicy::SOURCE_BINDING;
    public const GRPC = 'grpc';
    public const GRPC_WEB = 'grpc_web';
    public const HTTP_JSON = 'http_json';

    public static function select(
        string $family,
        bool $streaming = false,
        string $runtime = 'native',
        ?string $override = null,
        bool $bearerAuth = true,
        ?array $installed = null,
        ?array $endpoint = null,
    ): string
    {
        return GeneratedRemotePolicy::select(
            $family,
            $streaming,
            $runtime,
            $bearerAuth,
            $installed,
            $endpoint,
            $override,
        );
    }

    public static function validateBearer(string $token): string
    {
        return GeneratedRemotePolicy::validateBearer($token);
    }
}
