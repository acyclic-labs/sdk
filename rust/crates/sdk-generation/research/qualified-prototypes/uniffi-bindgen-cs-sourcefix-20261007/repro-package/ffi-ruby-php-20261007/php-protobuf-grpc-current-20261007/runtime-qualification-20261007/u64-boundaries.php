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
use Acyclic\Actors\V1\ActorLimits;
$values = ['0', '9223372036854775807', '9223372036854775808', '18446744073709551615'];
foreach ($values as $v) {
    try {
        $m = (new ActorLimits())->setHandlerTimeoutMillis($v);
        $wire = $m->serializeToString();
        $copy = new ActorLimits(); $copy->mergeFromString($wire);
        echo 'VALUE=' . $v . ' CONSTRUCT=PASS WIRE_BYTES=' . strlen($wire) . ' ROUNDTRIP=' . var_export($copy->getHandlerTimeoutMillis(), true) . PHP_EOL;
    } catch (Throwable $e) {
        echo 'VALUE=' . $v . ' CONSTRUCT=FAIL TYPE=' . get_class($e) . ' MESSAGE=' . $e->getMessage() . PHP_EOL;
    }
}
