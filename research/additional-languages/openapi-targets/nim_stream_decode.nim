import json
import options
import acyclic_stream_nim/models/model_acyclic_stream_v2_read_response

let value = to(parseJson("{\"record\":{\"value\":\"AQID\",\"sequence\":\"1\"}}"), AcyclicStreamV2ReadResponse)
echo value.record.isSome
