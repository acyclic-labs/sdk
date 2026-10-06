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
            $installed ?? GeneratedRemotePolicy::installedAvailability($this->runtime),
            GeneratedRemotePolicy::normalizeEndpoint($endpoint),
        );
        if ($bearer !== null) {
            RemotePolicy::validateBearer($bearer);
        }
        $this->invoker = Closure::fromCallable($invoker);
    }

    private readonly Closure $invoker;

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
        return ($this->invoker)($operation, $request, $this->transport);
    }
}
