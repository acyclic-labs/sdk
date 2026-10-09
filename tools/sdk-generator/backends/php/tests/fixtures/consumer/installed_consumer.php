<?php
require $argv[1];
use Google\Protobuf\Internal\FileDescriptorSet;
use Acyclic\Actors\V1\CreateActorRequest;
use Acyclic\Workers\V1\PublishVersionRequest;
use Acyclic\Stream\V2\AppendRequest;
use Acyclic\Stream\V2\ReadRequest;
function check($value,$message) { if(!$value) throw new RuntimeException($message); }
function normalizeDescriptor($message,$allowBuf=false) {
 $unknown=(new ReflectionProperty(Google\Protobuf\Internal\Message::class,'unknown'))->getValue($message);
 if($unknown!=='') {
  check($allowBuf,'unexpected nested descriptor field');
  $input=new Google\Protobuf\Internal\CodedInputStream($unknown);
  while(($tag=$input->readTag())!==0) {
   check($tag===(8042*8+2),'unexpected descriptor extension');
   check($input->readVarint32($size)&&$input->readRaw($size,$body),'invalid Buf image extension');
  }
 }
 $descriptor=Google\Protobuf\Internal\DescriptorPool::getGeneratedPool()->getDescriptorByClassName(get_class($message));
 foreach($descriptor->getField() as $field) {
  if($field->getType()!==11&&$field->getType()!==10)continue;
  $getter=$field->getGetter();$value=$message->$getter();
  if($field->isRepeated())foreach($value as $child)normalizeDescriptor($child);
  elseif($value!==null)normalizeDescriptor($value);
 }
 $message->discardUnknownFields();
}
function populated($message,$depth=0) {
 $descriptor=Google\Protobuf\Internal\DescriptorPool::getGeneratedPool()->getDescriptorByClassName(get_class($message));
 foreach($descriptor->getField() as $field) {
  if($field->isMap())continue;
  $type=$field->getType();
  if($type===11||$type===10) { if($depth>=2)continue;$class=$field->getMessageType()->getClass();$value=populated(new $class(),$depth+1); }
  elseif($type===9)$value='probe';
  elseif($type===12)$value="\x00\xff";
  elseif($type===8)$value=true;
  elseif($type===14)$value=$field->getEnumType()->getValueDescriptorByIndex($field->getEnumType()->getValueCount()-1)->getNumber();
  else $value=17;
  $setter=$field->getSetter();$message->$setter($field->isRepeated()?[$value]:$value);
 }
 $message->mergeFromString("\xf8\xff\xff\xff\x0f\x17");
 check($message->serializeToString()!=='','RPC sample empty');return $message;
}
class ProbeCall extends Grpc\AbstractCall {
 public function __construct($deserialize) { $this->deserialize=$deserialize; }
 public function decode($bytes) { return $this->_deserializeResponse($bytes); }
 public function encode($message) { return $this->_serializeMessage($message); }
}
trait Probe {
 public $expected,$package,$service,$calls=0;
 public function __construct() {}
 protected function _simpleRequest($method,$argument,$deserialize,array $metadata=[],array $options=[]) { return $this->capture(false,$method,$argument,$deserialize,$metadata,$options); }
 protected function _serverStreamRequest($method,$argument,$deserialize,array $metadata=[],array $options=[]) { return $this->capture(true,$method,$argument,$deserialize,$metadata,$options); }
 private function capture($streaming,$method,$argument,$deserialize,$metadata,$options) {
  $this->calls++;$expected=$this->expected;
  check($method==='/'.$this->package.'.'.$this->service.'/'.$expected->getName(),'client RPC path differs');
  check($streaming===$expected->getServerStreaming(),'client RPC streaming shape differs');
  check($metadata===['probe'=>['value']]&&$options===['probe'=>true],'client call options differ');
  $pool=Google\Protobuf\Internal\DescriptorPool::getGeneratedPool();
  check('.'.$pool->getDescriptorByClassName(get_class($argument))->getFullName()===$expected->getInputType(),'client request type differs');
  $class=ltrim($deserialize[0],'\\');$sample=populated(new $class());
  check('.'.$pool->getDescriptorByClassName($class)->getFullName()===$expected->getOutputType(),'client response type differs');
  $call=new ProbeCall($deserialize);
  check($call->encode($argument)===$argument->serializeToString(),'client request encoder changed content');
  $decoded=$call->decode($sample->serializeToString());
  check($decoded->serializeToString()===$sample->serializeToString(),'client response decoder changed content');
  return $argument->serializeToString();
 }
}
class ProbeActors extends Acyclic\Actors\V1\ActorsServiceClient { use Probe; }
class ProbeWorkers extends Acyclic\Workers\V1\WorkersServiceClient { use Probe; }
class ProbeStream extends Acyclic\Stream\V2\StreamServiceClient { use Probe; }
$probes=[new ProbeActors(),new ProbeWorkers(),new ProbeStream()];$index=0;
check(count($argv)===6,'pass installed autoload, SDK root and three Rust descriptors');
$installed=realpath($argv[2]);
foreach(['actors/v1/actors','workers/v1/workers','stream/v2/stream'] as $family) {
 $metadata=$installed.'/src/GPBMetadata/'.str_replace(' ','/',ucwords(str_replace('/',' ',$family))).'.php';
 $tokens=token_get_all(file_get_contents($metadata));$find=false;$literal=null;
 foreach($tokens as $token) {
  if(is_array($token)&&$token[0]===T_STRING&&$token[1]==='internalAddGeneratedFile') $find=true;
  elseif($find&&is_array($token)&&$token[0]===T_CONSTANT_ENCAPSED_STRING) { $literal=$token[1];break; }
 }
 check($literal!==null&&$literal[0]==='"','maintained metadata literal absent');
 $actual=new FileDescriptorSet();$actual->mergeFromString(stripcslashes(substr($literal,1,-1)));
 $rust=new FileDescriptorSet();$rust->mergeFromString(file_get_contents($argv[3+$index]));
 $expected=null;foreach($rust->getFile() as $file)if($file->getName()===$family.'.proto')$expected=$file;
 check($expected!==null&&count($actual->getFile())===1,'file descriptor selection differs');
 $found=$actual->getFile()[0];
 $expected->setSourceCodeInfo(null);normalizeDescriptor($expected,true);normalizeDescriptor($found);
 check($found->serializeToString()===$expected->serializeToString(),'file descriptor differs: '.$family);
 $service=$expected->getService()[0];$probe=$probes[$index++];$probe->package=$expected->getPackage();$probe->service=$service->getName();
 $clientClass=get_parent_class($probe);$reflection=new ReflectionClass($clientClass);
 $methods=array_filter($reflection->getMethods(),fn($method)=>$method->getDeclaringClass()->getName()===$clientClass&&$method->getName()!=='__construct');
 check(count($methods)===count($service->getMethod()),'client method count differs');
 check(str_starts_with(realpath($reflection->getFileName()),$installed.DIRECTORY_SEPARATOR),'client source is not installed');
 foreach($service->getMethod() as $method) {
  check(!$method->getClientStreaming(),'client streaming request control missing');
  $probe->expected=$method;$operation=$method->getName();$class=(string)$reflection->getMethod($operation)->getParameters()[0]->getType();$request=populated(new $class());
  check($probe->$operation($request,['probe'=>['value']],['probe'=>true])===$request->serializeToString(),'client request content differs');
 }
 check($probe->calls===count($service->getMethod()),'client call count differs');
}
$bytes="\x00\xff";
$actor=new CreateActorRequest(['code_sha256'=>$bytes,'home_region'=>'test','idempotency_key'=>'test']);
$worker=new PublishVersionRequest(['javascript_module'=>$bytes,'expected_sha256'=>$bytes,'idempotency_key'=>'test']);
$append=new AppendRequest(['path'=>'test/path','records'=>[$bytes,''],'if_tail'=>-1,'idempotency_key'=>$bytes]);
$read=new ReadRequest(['path'=>'test/path','from'=>-1,'limit'=>4294967295]);
check($actor->getCodeSha256()===$bytes&&$worker->getJavascriptModule()===$bytes&&$worker->getExpectedSha256()===$bytes,'bytes changed during construction');
check($append->getIfTail()===-1&&$read->getFrom()===-1&&$read->getLimit()===-1,'unsigned bits changed');
check(bin2hex($read->serializeToString())===bin2hex(chr(10).chr(9).'test/path'). '10ffffffffffffffffff0118ffffffff0f','unsigned maximum wire bits differ');
foreach([$actor,$worker,$append,$read] as $message) {
 $class=get_class($message);$restored=new $class();$restored->mergeFromString($message->serializeToString());
 check($restored->serializeToString()===$message->serializeToString(),'wire round trip differs');
 check(str_starts_with(realpath((new ReflectionClass($class))->getFileName()),$installed.DIRECTORY_SEPARATOR),'message source is not installed');
}
$append->clearIfTail();check(!$append->hasIfTail(),'optional absence failed');$append->setIfTail(0);check($append->hasIfTail(),'optional zero lost');
$zero=new AppendRequest();$zero->setIfTail(0);check($zero->serializeToString()==="\x18\x00",'optional zero wire differs');
$observation=new Acyclic\Stream\V2\IdempotencyObservation();
$observation->setAppend(new Acyclic\Stream\V2\AppendResponse());check($observation->getOutcome()==='append','append oneof branch missing');
$observation->setFork(new Acyclic\Stream\V2\ForkReceipt());check($observation->getOutcome()==='fork'&&$observation->getAppend()===null,'oneof switching failed');
$observation->setFork(null);check($observation->getOutcome()==='','oneof clearing failed');
echo "PASS: installed PHP descriptors, bytes, unsigned bits, optional zero, oneof and client RPC shapes\n";
