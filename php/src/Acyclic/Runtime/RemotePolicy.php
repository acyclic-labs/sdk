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

    public static function resolveRuntime(?string $runtime = 'auto'): string
    {
        $value = strtolower($runtime ?? 'auto');
        if ($value !== 'auto') {
            return $value;
        }

        // Installed PHP runs natively. An embedded PHP/WASM host can opt into
        // the generated browser policy through its host bridge.
        return getenv('ACYCLIC_PHP_EMBEDDED_BROWSER') === '1' ? 'browser' : 'native';
    }

    public static function select(
        string $family,
        bool $streaming = false,
        string $runtime = 'auto',
        ?string $override = null,
        bool $bearerAuth = true,
        ?array $installed = null,
        ?array $endpoint = null,
    ): string
    {
        return GeneratedRemotePolicy::select(
            $family,
            $streaming,
            self::resolveRuntime($runtime),
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
