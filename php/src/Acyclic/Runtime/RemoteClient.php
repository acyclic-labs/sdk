<?php

declare(strict_types=1);

namespace Acyclic\Runtime;

use Closure;

/** Thin typed-boundary-neutral facade; the injected invoker owns wire encoding. */
final class RemoteClient
{
    private readonly string $transport;
    private readonly string $runtime;

    public function __construct(
        private readonly string $family,
        callable $invoker,
        string $runtime = 'auto',
        bool $streaming = false,
        ?string $transport = null,
        ?string $bearer = null,
        bool $bearerAuth = true,
        ?array $installed = null,
        array|string|null $endpoint = null,
    ) {
        $this->runtime = RemotePolicy::resolveRuntime($runtime);
        $this->transport = RemotePolicy::select(
            $family,
            $streaming,
            $this->runtime,
            $transport,
            $bearerAuth,
            $installed ?? self::installedAvailability($this->runtime),
            self::normalizeEndpoint($endpoint),
        );
        if ($bearer !== null) {
            RemotePolicy::validateBearer($bearer);
        }
        $this->invoker = Closure::fromCallable($invoker);
    }

    private readonly Closure $invoker;

    /** @return array{grpc: bool, grpc_web: bool, http_json: bool} */
    private static function installedAvailability(string $runtime): array
    {
        if ($runtime === 'browser') {
            return ['grpc' => false, 'grpc_web' => false, 'http_json' => true];
        }
        return ['grpc' => extension_loaded('grpc'), 'grpc_web' => false, 'http_json' => true];
    }

    /** @return array{grpc: bool, grpc_web: bool, http_json: bool} */
    private static function normalizeEndpoint(array|string|null $endpoint): array
    {
        if (is_array($endpoint)) {
            return $endpoint;
        }
        if ($endpoint === null) {
            $value = getenv('ACYCLIC_ENDPOINT_TRANSPORTS');
            if ($value === false || trim($value) === '') {
                return ['grpc' => true, 'grpc_web' => true, 'http_json' => true];
            }
            $kinds = array_fill_keys(array_map('trim', explode(',', strtolower($value))), true);
            return [
                'grpc' => isset($kinds['grpc']),
                'grpc_web' => isset($kinds['grpc_web']),
                'http_json' => isset($kinds['http_json']),
            ];
        }
        $scheme = strtolower((string) parse_url($endpoint, PHP_URL_SCHEME));
        return match ($scheme) {
            'grpc' => ['grpc' => true, 'grpc_web' => false, 'http_json' => false],
            'grpc-web' => ['grpc' => false, 'grpc_web' => true, 'http_json' => false],
            'http', 'https' => ['grpc' => false, 'grpc_web' => false, 'http_json' => true],
            default => ['grpc' => true, 'grpc_web' => true, 'http_json' => true],
        };
    }

    public function transport(): string
    {
        return $this->transport;
    }

    public function runtime(): string
    {
        return $this->runtime;
    }

    public function call(string $operation, mixed $request): mixed
    {
        if (class_exists('Acyclic\\TypePolicy\\Wire')) {
            $request = \Acyclic\TypePolicy\Wire::normalizeRequest($this->family, $request);
        }
        return ($this->invoker)($operation, $request, $this->transport);
    }

    public function typedField(mixed $response, string $field): mixed
    {
        if (!class_exists('Acyclic\\TypePolicy\\Wire')) {
            return $response;
        }
        $getter = 'get' . str_replace(' ', '', ucwords(str_replace('_', ' ', $field)));
        $value = is_array($response)
            ? ($response[$field] ?? null)
            : (method_exists($response, $getter) ? $response->{$getter}() : (method_exists($response, $field) ? $response->{$field}() : null));
        return \Acyclic\TypePolicy\Wire::typedField($this->family, $field, $value);
    }
}
