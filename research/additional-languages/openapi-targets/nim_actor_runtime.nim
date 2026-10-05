import httpclient
import options
import acyclic_actors_nim
import acyclic_actors_nim/models/model_byte_array

let http = newHttpClient()
http.headers["Authorization"] = "Bearer bash-fixture-token"
var request: AcyclicActorsV1InvokeActorRequest
request.body = some(ByteArray("AQID"))
let (value, response) = invokeActor(http, request)
doAssert response.code == Http200
doAssert value.isSome
doAssert value.get.body.isSome
doAssert string(value.get.body.get) == "AQID"
echo "nim-actor-pass"
