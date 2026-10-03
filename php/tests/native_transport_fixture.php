<?php
require dirname(__DIR__) . '/vendor/autoload.php';
$endpoint = getenv('FIXTURE_GRPC_ADDRESS') ?: '127.0.0.1:58151';
$limits = new \Acyclic\Actors\V1\ActorLimits();
$limits->setHandlerTimeoutMillis(1000);
$limits->setMemoryBytes(4096);
$limits->setCheckpointBytes(4096);
$request = new \Acyclic\Actors\V1\CreateActorRequest();
$request->setCodeSha256(str_repeat('a', 32));
$request->setHomeRegion('fixture');
$request->setLimits($limits);
$request->setIdempotencyKey('php-native-actor-1');
$client = new \Acyclic\Actors\V1\ActorsServiceClient($endpoint, ['credentials' => \Grpc\ChannelCredentials::createInsecure()]);
list($response, $status) = $client->CreateActor($request)->wait();
if ($status->code !== \Grpc\STATUS_OK) { throw new RuntimeException("actor status {$status->code}: {$status->details}"); }
$actor = $response->getActor();
echo "unary status={$status->code} actor=".$actor->getActorId()." region=".$actor->getHomeRegion()."\n";
$stream = new \Acyclic\Stream\V2\StreamServiceClient($endpoint, ['credentials' => \Grpc\ChannelCredentials::createInsecure()]);
$append = new \Acyclic\Stream\V2\AppendRequest();
$append->setPath('php-native-stream');
$append->setIdempotencyKey('php-native-append-1');
$append->setRecords(['php-record']);
list($outcome, $status) = $stream->Append($append)->wait();
echo "stream-unary status={$status->code}\n";
$read = new \Acyclic\Stream\V2\ReadRequest();
$read->setPath('php-native-stream');
$read->setFrom(0);
$read->setLimit(10);
$call = $stream->Read($read);
$count = 0;
foreach ($call->responses() as $item) { $count++; $call->cancel(); break; }
echo "stream-cancel issued count=$count\n";
