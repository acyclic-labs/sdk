<?php

declare(strict_types=1);

namespace Acyclic\Runtime;

use InvalidArgumentException;

require_once __DIR__ . '/GeneratedRemotePolicy.php';

/** Compatibility facade over the Rust-emitted transport policy. */
final class RemotePolicy
{
    public const SOURCE_BINDING = GeneratedRemotePolicy::SOURCE_BINDING;
    public const GRPC = GeneratedRemotePolicy::GRPC;
    public const GRPC_WEB = GeneratedRemotePolicy::GRPC_WEB;
    public const HTTP_JSON = GeneratedRemotePolicy::HTTP_JSON;

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

    public static function resolveRuntime(string $runtime = 'auto'): string
    {
        return GeneratedRemotePolicy::resolveRuntime($runtime);
    }
}
