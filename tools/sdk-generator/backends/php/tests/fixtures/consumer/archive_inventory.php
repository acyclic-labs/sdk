<?php
// Inspect with maintained libzip before Composer extracts any admitted archive.
// Individual bodies travel in chunks; neither PHP nor stdout holds the ZIP.
if(count($argv)!==3)throw new RuntimeException('pass archive and inspection output');
$zip=new ZipArchive();
if($zip->open($argv[1],ZipArchive::CHECKCONS)!==true)throw new RuntimeException('invalid dependency ZIP');
if($zip->numFiles<1)throw new RuntimeException('empty dependency ZIP');
$output=fopen($argv[2],'xb');if($output===false)throw new RuntimeException('cannot create inspection output');
function emit($value) {
 global $output;
 $line=json_encode($value,JSON_THROW_ON_ERROR)."\n";
 for($offset=0;$offset<strlen($line);$offset+=$written) {
  $written=fwrite($output,substr($line,$offset));
  if($written===false||$written===0)throw new RuntimeException('cannot write inspection output');
 }
}
try {
 for($i=0;$i<$zip->numFiles;$i++) {
  $stat=$zip->statIndex($i);if($stat===false||!is_int($stat['size'])||$stat['size']<0)throw new RuntimeException('invalid ZIP member');
  if(!$zip->getExternalAttributesIndex($i,$system,$attributes))throw new RuntimeException('ZIP attributes missing');
  $type=($attributes>>16)&0170000;
  if(!in_array($type,[0,0100000,0040000],true))throw new RuntimeException('ZIP contains a link or special file');
  $directory=str_ends_with($stat['name'],'/');
  if($directory&&$stat['size']!==0)throw new RuntimeException('ZIP directory has contents');
  emit(['path'=>$stat['name'],'directory'=>$directory,'size'=>$stat['size']]);
  if(!$directory) {
   $source=$zip->getStreamIndex($i);if($source===false)throw new RuntimeException('cannot read ZIP member');
   $remaining=$stat['size'];$crc=hash_init('crc32b');
   try {
    while(!feof($source)) {
     $bytes=fread($source,64*1024);
     if($bytes===false)throw new RuntimeException('cannot read ZIP contents');
     if($bytes==='') { if(!feof($source))throw new RuntimeException('ZIP read made no progress');break; }
     if(strlen($bytes)>$remaining)throw new RuntimeException('ZIP contents length differs');
     $remaining-=strlen($bytes);hash_update($crc,$bytes);
     emit(['data'=>base64_encode($bytes)]);
    }
   } finally { fclose($source); }
   if($remaining!==0||hash_final($crc)!==sprintf('%08x',$stat['crc']))throw new RuntimeException('ZIP contents differ');
  }
  emit(['end'=>true]);
 }
 if(!$zip->close()||!fflush($output))throw new RuntimeException('cannot finish ZIP inspection');
} finally { fclose($output); }
