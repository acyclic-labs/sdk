<?php
declare(strict_types=1);
require '/tmp/php-composer-current/vendor/autoload.php';
$generated='/mnt/c/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/.tmp-current-php-generated3/';
spl_autoload_register(static function(string $class)use($generated):void{foreach(['Acyclic\\'=>'Acyclic/','GPBMetadata\\'=>'GPBMetadata/']as$prefix=>$path){if(str_starts_with($class,$prefix)){ $f=$generated.$path.str_replace('\\','/',substr($class,strlen($prefix))).'.php';if(is_file($f))require$f;}}});
use Acyclic\Actors\V1\ActorsServiceClient; use Acyclic\Actors\V1\InspectActorRequest;
$c=new ActorsServiceClient(trim(file_get_contents('/tmp/actors-php-fixture/endpoint')),['credentials'=>Grpc\ChannelCredentials::createInsecure()]);
[$r,$s]=$c->InspectActor((new InspectActorRequest())->setActorId('u64'),['authorization'=>['Bearer php-fixture-token']])->wait();
try { $a=$r->getActor(); echo 'STATUS='.$s->code.' VALUE='.var_export($a->getConfigurationRevision(),true).' HAS='.var_export($a->hasCheckpointUnixMillis(),true).PHP_EOL; } catch(Throwable $e) { echo 'STATUS='.$s->code.' DECODE_FAIL='.get_class($e).' '.$e->getMessage().PHP_EOL; }
