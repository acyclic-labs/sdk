using Test

const projection_path = ARGS[1]
const base_url = length(ARGS) >= 2 ? ARGS[2] : get(ENV, "ACYCLIC_FAMILY_FIXTURE_URL", "http://127.0.0.1:18770")
include(projection_path)
using .AcyclicHttpProjection

@testset "Rust-owned Julia family HTTP consumer" begin
    @test select_transport() == :http_json
    @test_throws ArgumentError select_transport(:grpc)

    unauthorized = try
        request_json(base_url, 1)
        nothing
    catch error
        error
    end
    @test unauthorized isa ApiError
    @test unauthorized.status == 401

    recovered = request_with_recovery(
        base_url,
        1;
        bearer_token="fixture-token",
        attempts=2,
    )
    @test recovered.status == 200
    @test recovered.policy.rpc == POLICIES[1].rpc

    for index in eachindex(ROUTES)
        response = request_json(base_url, index; bearer_token="fixture-token")
        @test response.status == 200
        @test response.route.operation_id == ROUTES[index].operation_id
        @test response.policy.rpc == ROUTES[index].rpc
    end

    streaming_index = findfirst(route -> route.client_streaming || route.server_streaming, ROUTES)
    if streaming_index !== nothing
        lines = request_stream(base_url, streaming_index; bearer_token="fixture-token")
        @test !isempty(lines)
    end

    token = CancellationToken()
    cancel!(token)
    @test_throws InterruptException request_json(
        base_url,
        1;
        bearer_token="fixture-token",
        token,
    )
end

println("JULIA_FAMILY_CONSUMER_PASS|", FAMILY, "|routes=", length(ROUTES), "|policies=", length(POLICIES))
