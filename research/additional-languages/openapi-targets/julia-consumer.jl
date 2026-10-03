using AcyclicWorkers
using Test

const BASE_URL = get(ENV, "ACYCLIC_FIXTURE_URL", "http://127.0.0.1:18769")

@testset "Rust-owned Workers Julia package consumer" begin
    response = invoke_deployment(BASE_URL, "prod", InvokeDeploymentRequest(UInt8[1, 2, 3]))
    @test response.status == 200
    @test response.body == UInt8[0x6f, 0x6b]
    @test response.resolved_sha256 == UInt8[1, 2, 3]
    @test response.resolved_revision == UInt64(18446744073709551615)

    api_error = try
        invoke_deployment(BASE_URL, "error", InvokeDeploymentRequest(UInt8[1, 2, 3]))
        nothing
    catch ex
        ex
    end
    @test api_error isa ApiError
    @test api_error.status == 409
    @test occursin("CONFLICT", api_error.body)

    token = CancellationToken()
    cancel!(token)
    @test_throws InterruptException invoke_deployment(
        BASE_URL,
        "prod",
        InvokeDeploymentRequest(UInt8[1, 2, 3]);
        token,
    )
end
