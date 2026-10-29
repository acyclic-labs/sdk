`pi-assistant-error.jsonl` contains unchanged assistant `message_end` and
`agent_settled` lines captured
from Pi 1.1.0 on Linux with `--mode json --print --no-session`, the local
Responses provider fixture, and no authentication. The real CLI exited 0
despite this error and dispatched no model request. The capture contains no
credentials or filesystem paths.

SHA-256: `02b1bdc90f4ace0499a89dbcc3c8dc407eb805f9d16f4c51e71b64b90aba1fd7`.
