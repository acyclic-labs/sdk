import httpclient
import options

import acyclic_actors_nim
import acyclic_stream_nim
import acyclic_actors_nim/models/model_byte_array

let http = newHttpClient()
http.headers["Authorization"] = "Bearer bash-fixture-token"

var actorRequest: AcyclicActorsV1InvokeActorRequest
actorRequest.body = some(ByteArray("AQID"))
let (actorValue, actorResponse) = invokeActor(http, actorRequest)
doAssert actorResponse.code == Http200
doAssert actorValue.isSome
doAssert actorValue.get.body.isSome
doAssert string(actorValue.get.body.get) == "AQID"

let retryHttp = newHttpClient()
retryHttp.headers["Authorization"] = "Bearer bash-fixture-token"
var streamRetry: AcyclicStreamV2ReadRequest
streamRetry.path = some("root")
streamRetry.limit = some(0)
let (retryValue, retryResponse) = read(retryHttp, streamRetry)
doAssert retryResponse.code == Http503
doAssert retryValue.isNone

let recoveryHttp = newHttpClient()
recoveryHttp.headers["Authorization"] = "Bearer bash-fixture-token"
var streamRequest: AcyclicStreamV2ReadRequest
streamRequest.path = some("root")
streamRequest.limit = some(1)
let (streamValue, streamResponse) = read(recoveryHttp, streamRequest)
doAssert streamResponse.code == Http200
doAssert streamValue.isSome

echo "nim-runtime-pass actors=200 stream=503->200"
