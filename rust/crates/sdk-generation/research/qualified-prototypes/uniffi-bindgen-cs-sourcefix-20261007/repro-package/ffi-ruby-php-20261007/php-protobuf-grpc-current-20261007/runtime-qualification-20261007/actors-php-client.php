<?php
declare(strict_types=1);
require '/tmp/php-composer-current/vendor/autoload.php';
$generated = '/mnt/c/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/.tmp-current-php-generated3/';
spl_autoload_register(static function (string $class) use ($generated): void {
    foreach (['Acyclic\\' => 'Acyclic/', 'GPBMetadata\\' => 'GPBMetadata/'] as $prefix => $path) {
        if (str_starts_with($class, $prefix)) {
            $relative = str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
            $file = $generated . $path . $relative;
            if (is_file($file)) { require $file; }
            return;
        }
    }
});

use Acyclic\Actors\V1\ActorsServiceClient;
use Acyclic\Actors\V1\ActorLimits;
use Acyclic\Actors\V1\AddSubscriptionRequest;
use Acyclic\Actors\V1\CheckpointActorRequest;
use Acyclic\Actors\V1\CreateActorRequest;
use Acyclic\Actors\V1\InspectActorRequest;
use Acyclic\Actors\V1\InvokeActorRequest;
use Acyclic\Actors\V1\RemoveSubscriptionRequest;
use Acyclic\Actors\V1\ResumeSubscriptionRequest;
use Acyclic\Actors\V1\SubscriptionSpec;
use Acyclic\Actors\V1\UpdateActorRequest;

$endpoint = trim(file_get_contents('/tmp/actors-php-fixture/endpoint'));
$client = new ActorsServiceClient($endpoint, ['credentials' => Grpc\ChannelCredentials::createInsecure()]);
$metadata = ['authorization' => ['Bearer php-fixture-token']];
$max = '9223372036854775807';

function call_ok(ActorsServiceClient $client, string $name, object $request, array $metadata): object {
    [$response, $status] = $client->{$name}($request, $metadata)->wait();
    if ($status->code !== Grpc\STATUS_OK) {
        throw new RuntimeException($name . ' failed: ' . $status->code . ' ' . $status->details);
    }
    echo "RPC_PASS={$name}\n";
    return $response;
}

$limits = (new ActorLimits())->setHandlerTimeoutMillis($max)->setMemoryBytes($max)->setCheckpointBytes($max);
$create = (new CreateActorRequest())->setCodeSha256('php')->setHomeRegion('fixture')->setLimits($limits)->setIdempotencyKey('php-create');
call_ok($client, 'CreateActor', $create, $metadata);
$update = (new UpdateActorRequest())->setActorId('php')->setExpectedConfigurationRevision($max)->setIdempotencyKey('php-update');
call_ok($client, 'UpdateActor', $update, $metadata);
$inspect = (new InspectActorRequest())->setActorId('u64');
$inspect_response = call_ok($client, 'InspectActor', $inspect, $metadata);
$actor = $inspect_response->getActor();
if ((string) $actor->getConfigurationRevision() !== $max || !$actor->hasCheckpointUnixMillis()) {
    throw new RuntimeException('u64 or optional presence mismatch');
}
echo "U64_VALUE=" . (string) $actor->getConfigurationRevision() . " OPTIONAL_PRESENT=1\n";
$add = (new AddSubscriptionRequest())->setActorId('php')->setSubscription(new SubscriptionSpec())->setIdempotencyKey('php-add');
call_ok($client, 'AddSubscription', $add, $metadata);
$remove = (new RemoveSubscriptionRequest())->setActorId('php')->setSubscriptionId('sub')->setIdempotencyKey('php-remove');
call_ok($client, 'RemoveSubscription', $remove, $metadata);
$resume = (new ResumeSubscriptionRequest())->setActorId('php')->setSubscriptionId('sub')->setIdempotencyKey('php-resume');
call_ok($client, 'ResumeSubscription', $resume, $metadata);
$checkpoint = (new CheckpointActorRequest())->setActorId('php')->setIdempotencyKey('php-checkpoint');
call_ok($client, 'CheckpointActor', $checkpoint, $metadata);
$invoke = (new InvokeActorRequest())->setActorId('php')->setMethod('GET')->setUrl('fixture');
call_ok($client, 'InvokeActor', $invoke, $metadata);

[, $unauth_status] = $client->InspectActor($inspect)->wait();
if ($unauth_status->code !== Grpc\STATUS_UNAUTHENTICATED) { throw new RuntimeException('unauthorized status mismatch: ' . $unauth_status->code); }
echo "UNAUTHENTICATED_STATUS_PASS={$unauth_status->code}\n";
$error_request = (new InspectActorRequest())->setActorId('error');
[, $error_status] = $client->InspectActor($error_request, $metadata)->wait();
if ($error_status->code !== Grpc\STATUS_PERMISSION_DENIED) { throw new RuntimeException('server abort status mismatch: ' . $error_status->code); }
echo "SERVER_ABORT_STATUS_PASS={$error_status->code}\n";

$slow = (new InvokeActorRequest())->setActorId('php')->setMethod('sleep')->setUrl('fixture');
$pending = $client->InvokeActor($slow, $metadata);
usleep(300000);
$pending->cancel();
[, $cancel_status] = $pending->wait();
if ($cancel_status->code !== Grpc\STATUS_CANCELLED) { throw new RuntimeException('cancel status mismatch: ' . $cancel_status->code); }
echo "CANCEL_STATUS_PASS={$cancel_status->code}\n";
