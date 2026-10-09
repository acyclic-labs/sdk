<?php
require $argv[1];
(new Acyclic\Workers\V1\PublishVersionRequest())->setJavascriptModule([]);
