import httpclient
import json
import options
import acyclic_actors_nim

let http = newHttpClient()
http.headers["Authorization"] = "Bearer bash-fixture-token"
var request: AcyclicActorsV1InvokeActorRequest
request.body = some("AQID")
let requestJson = $(%request)
echo requestJson
let response = http.post("http://127.0.0.1:18768/v1/actors/invoke", requestJson)
echo response.code
echo response.body
