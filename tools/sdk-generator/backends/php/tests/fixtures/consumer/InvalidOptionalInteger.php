<?php
require $argv[1];
(new Acyclic\Stream\V1\AppendRequest())->setIfTail([]);
