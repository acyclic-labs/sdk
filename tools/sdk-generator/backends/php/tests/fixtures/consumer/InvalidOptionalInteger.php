<?php
require $argv[1];
(new Acyclic\Stream\V2\AppendRequest())->setIfTail([]);
