<?php
// Inspect with maintained libzip before Composer extracts any admitted archive.
if(count($argv)!==2)throw new RuntimeException('pass one archive');
$zip=new ZipArchive();
if($zip->open($argv[1],ZipArchive::CHECKCONS)!==true)throw new RuntimeException('invalid dependency ZIP');
if($zip->numFiles<1||$zip->numFiles>20000)throw new RuntimeException('ZIP member count exceeds bound');
$entries=[];$expanded=0;
for($i=0;$i<$zip->numFiles;$i++) {
 $stat=$zip->statIndex($i);if($stat===false)throw new RuntimeException('invalid ZIP member');
 $expanded+=$stat['size'];if($expanded>32*1024*1024)throw new RuntimeException('ZIP expansion exceeds bound');
 if(!$zip->getExternalAttributesIndex($i,$system,$attributes))throw new RuntimeException('ZIP attributes missing');
 $type=($attributes>>16)&0170000;
 if(!in_array($type,[0,0100000,0040000],true))throw new RuntimeException('ZIP contains a link or special file');
 $directory=str_ends_with($stat['name'],'/');
 if($directory) { if($stat['size']!==0)throw new RuntimeException('ZIP directory has contents');$bytes=''; }
 else {
  $bytes=$zip->getFromIndex($i);if($bytes===false||strlen($bytes)!==$stat['size']||hash('crc32b',$bytes)!==sprintf('%08x',$stat['crc']))throw new RuntimeException('ZIP contents differ');
 }
 $entries[]=['path'=>$stat['name'],'directory'=>$directory,'data'=>base64_encode($bytes)];
}
$zip->close();echo json_encode($entries,JSON_THROW_ON_ERROR);
