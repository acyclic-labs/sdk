<?php
require getenv('PHP_PACKAGE') . '/vendor/autoload.php';
$endpoint = getenv('FIXTURE_GRPC_ADDRESS');
$path = getenv('RECOVERY_PATH');
$id = getenv('RECOVERY_ID');
$stream = new \Acyclic\Stream\V2\StreamServiceClient($endpoint, ['credentials' => \Grpc\ChannelCredentials::createInsecure()]);
$append = new \Acyclic\Stream\V2\AppendRequest();
$append->setPath($path); $append->setRecords(['php-recovery-0', 'php-recovery-1']); $append->setIfTail(0); $append->setIdempotencyKey('php-recovery-status-' . $id);
list($response, $status) = $stream->Append($append)->wait();
if ($status->code !== \Grpc\STATUS_OK) throw new RuntimeException('append status');
$follow = new \Acyclic\Stream\V2\FollowRequest();
$follow->setPath($path); $follow->setFrom(0);
$call = $stream->Follow($follow);
$first = null;
foreach ($call->responses() as $item) { $first = $item->getRecord()->getSequence(); $call->cancel(); break; }
$cancelStatus = $call->getStatus();
$stream->close();
unset($call, $stream);
if ($first !== 0 || $cancelStatus->code !== \Grpc\STATUS_CANCELLED) throw new RuntimeException('cancel status mismatch');
echo 'cancel_status=' . $cancelStatus->code . ':' . $cancelStatus->details . PHP_EOL;
