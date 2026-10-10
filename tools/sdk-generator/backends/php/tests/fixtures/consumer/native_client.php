<?php
require $argv[1];
foreach([Acyclic\Actors\V1\ActorsServiceClient::class,Acyclic\Workers\V1\WorkersServiceClient::class,Acyclic\Stream\V1\StreamServiceClient::class] as $class) {
 $client=new $class('127.0.0.1:1',['credentials'=>Grpc\ChannelCredentials::createInsecure()]);
 $client->close();
}
echo "PASS: native PHP gRPC client creation and shutdown\n";
