$ErrorActionPreference = "Stop"
$root = "C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk"
$manifest = Join-Path $root "work/rpd-rust-106-manifest-current-20261005.json"
$result = Join-Path $root "work/rpd-ruby-106-result-current.json"
$fixture = Join-Path $root "work/rpd-fixture-current-target/debug/fixture-server.exe"
$ruby = "Q:\sdk\ruby-cache\ruby\rubyinstaller-3.2.11-1-x64\bin\ruby.exe"
$run = Get-Content $result -Raw | ConvertFrom-Json
$receipt = [ordered]@{
  schema = "acyclic.sdk.ruby.installed-106-rpc.source-bound.v3"
  generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  status = "source-bound-observed-failed"
  qualification_input = $false
  source_binding = [ordered]@{
    source_git_sha = (git -C $root rev-parse HEAD)
    rust_manifest = "work/rpd-rust-106-manifest-current-20261005.json"
    rust_manifest_sha256 = (Get-FileHash $manifest -Algorithm SHA256).Hash.ToLower()
    rust_manifest_complete = $true
    rust_record_count = 106
    canonical_fixture_commits = @("95cf11c7b5c443bfeaf06b40193aeccf7c4db5f0", "1923135038ef0fd51f7bda753c20b1ff142b1748")
    fixture_binary = "work/rpd-fixture-current-target/debug/fixture-server.exe"
    fixture_binary_sha256 = (Get-FileHash $fixture -Algorithm SHA256).Hash.ToLower()
    fixture_source_closure = "sha256:d236cd484f677fa1f8b38be0c1feb4d36ce39a76ee4816d9673818ea64ab43c5"
    rust_machine_compatibility_patch = "machines create request uses Require when required capability includes LiveCheckpoint"
    harness_handshake_source_fix = "filesystem harness observations use protocol.v1 handshake message identities"
  }
  runtime = [ordered]@{
    ruby_executable = $ruby
    ruby_sha256 = (Get-FileHash $ruby -Algorithm SHA256).Hash.ToLower()
    ruby_version = "3.2.11"
    grpc = "1.82.0"
    google_protobuf = "4.33.0"
    gem_home = "Q:\sdk\ruby-cache\gems"
  }
  execution = [ordered]@{
    endpoint = "127.0.0.1:52005"
    transport = "grpc"
    client = "Acyclic::Remote::Client"
    streaming_classification = "Rust-owned exact RPC identities"
    actual_public_request_construction = $true
    request_wire_from_rust_manifest = $true
    response_hashes_compared = $true
    cancellation = "server-streaming operation cancelled in ensure; Follow timed out before first frame"
    receipt = "work/rpd-ruby-106-result-current.json"
    receipt_sha256 = (Get-FileHash $result -Algorithm SHA256).Hash.ToLower()
  }
  counts = [ordered]@{
    status = $run.status
    record_count = $run.record_count
    passed = $run.passed
    response_mismatch = $run.response_mismatch
    failed = $run.failed
  }
  observed_failures = @($run.results | Where-Object { $_.status -ne "passed" } | ForEach-Object {
    [ordered]@{ index = $_.index; rpc = $_.rpc; status = $_.status; error_class = $_.error_class; error = $_.error }
  })
  evidence = [ordered]@{
    manifest = "Rust executable fixture manifest complete with 106 records and no missing RPCs"
    positive_rule = "passed requires public client.call, actual gRPC response, and exact response frame SHA256"
    negative_rule = "observed-status cases pass only on expected non-OK gRPC outcome"
    limitations = "Ruby package currently reaches 81/106 exact response outcomes; remaining mismatches and non-OK results are retained as failures"
    no_claim = "This receipt does not qualify Ruby for the 106-operation release gate"
  }
}
$out = Join-Path $root "research/acceptance/ruby-php-dart/qualification/ruby-installed-106-source-bound-20261005.receipt.json"
$receipt | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $out -Encoding utf8
Write-Output $out
