<?php
declare(strict_types=1);
require '/tmp/php-composer-current/vendor/autoload.php';
$generated = '/mnt/c/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/.tmp-current-php-generated3/';
spl_autoload_register(static function (string $class) use ($generated): void {
    foreach (['Acyclic\\' => 'Acyclic/', 'GPBMetadata\\' => 'GPBMetadata/'] as $prefix => $path) {
        if (str_starts_with($class, $prefix)) {
            $file = $generated . $path . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
            if (is_file($file)) require $file;
        }
    }
});
use Acyclic\Actors\V1\ActorsServiceClient;
use Acyclic\Actors\V1\CreateActorRequest;
use Acyclic\Actors\V1\InspectActorRequest;
use Acyclic\Actors\V1\SubscriptionSpec;
$param = (new ReflectionMethod(ActorsServiceClient::class, 'CreateActor'))->getParameters()[0]->getType();
if ((string)$param !== CreateActorRequest::class) throw new RuntimeException('request type mismatch');
echo "REQUEST_TYPE_PASS=" . $param . PHP_EOL;
$client = new ActorsServiceClient('127.0.0.1:1', ['credentials' => Grpc\ChannelCredentials::createInsecure()]);
try { $client->CreateActor(new InspectActorRequest()); throw new RuntimeException('wrong request accepted'); }
catch (TypeError $e) { echo "WRONG_REQUEST_REJECT_PASS=TypeError" . PHP_EOL; }
try { (new CreateActorRequest())->setLimits(new SubscriptionSpec()); throw new RuntimeException('wrong nested message accepted'); }
catch (TypeError $e) { echo "WRONG_NESTED_MESSAGE_REJECT_PASS=" . $e->getMessage() . PHP_EOL; }

