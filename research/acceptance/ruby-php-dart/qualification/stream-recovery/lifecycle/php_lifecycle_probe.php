<?php
require 'Q:/sdk/php-cache/package-final/vendor/autoload.php';
$case = getenv('PHP_CASE');
$endpoint = getenv('FIXTURE_GRPC_ADDRESS'); $path = getenv('RECOVERY_PATH'); $id = getenv('RECOVERY_ID');
$client = new \Acyclic\Stream\V2\StreamServiceClient($endpoint, ['credentials' => \Grpc\ChannelCredentials::createInsecure()]);
$append = new \Acyclic\Stream\V2\AppendRequest();
$append->setPath($path); $append->setRecords(['lifecycle-0', 'lifecycle-1']); $append->setIfTail(0); $append->setIdempotencyKey('lifecycle-' . $id);
list($response, $status) = $client->Append($append)->wait();
if ($status->code !== \Grpc\STATUS_OK) throw new RuntimeException('append failed');
$follow = new \Acyclic\Stream\V2\FollowRequest(); $follow->setPath($path); $follow->setFrom(0);
$call = $client->Follow($follow); $first = null;
foreach ($call->responses() as $item) { $first = $item->getRecord()->getSequence(); break; }
$call->cancel();
if ($case === 'close-before-status') { $client->close(); }
$cancelStatus = $call->getStatus();
echo 'first=' . $first . ' status=' . $cancelStatus->code . ':' . $cancelStatus->details . PHP_EOL;
if ($case === 'close-after-status' || $case === 'close-before-status') { $client->close(); }
if ($case === 'unset') { unset($call, $client); gc_collect_cycles(); }
if ($case === 'close-after-status') { unset($call, $client); gc_collect_cycles(); }
