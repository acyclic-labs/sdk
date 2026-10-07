<?php
declare(strict_types=1);
require '/tmp/php-composer-current/vendor/autoload.php';
$generated = '/mnt/c/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/.tmp-current-php-generated3/';
spl_autoload_register(static function (string $class) use ($generated): void {
    foreach (['Acyclic\\' => 'Acyclic/', 'GPBMetadata\\' => 'GPBMetadata/'] as $prefix => $path) {
        if (str_starts_with($class, $prefix)) { $file=$generated.$path.str_replace('\\','/',substr($class,strlen($prefix))).'.php'; if (is_file($file)) require $file; }
    }
});
use Acyclic\Actors\V1\ActorLimits;
use Brick\Math\BigInteger;
$v=BigInteger::of('18446744073709551615');
try { $m=(new ActorLimits())->setHandlerTimeoutMillis($v); echo "BRICK_BIGINT_CLIENT_VALUE=PASS\n"; }
catch (Throwable $e) { echo "BRICK_BIGINT_CLIENT_VALUE=FAIL TYPE=".get_class($e)." MESSAGE=".$e->getMessage()."\n"; }
echo "BRICK_BIGINT_STRING=".$v->toBase(10)."\n";
