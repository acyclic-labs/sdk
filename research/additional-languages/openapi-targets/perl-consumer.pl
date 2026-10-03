use strict;
use warnings;
use JSON qw(encode_json);
use WWW::OpenAPIClient::ApiClient;
use WWW::OpenAPIClient::Configuration;
use WWW::OpenAPIClient::DefaultApi;

my $family = $ARGV[0] // 'actors';
my $base_url = ($ENV{ACYCLIC_BASE_URL} // 'http://127.0.0.1:18766');
my $configuration = WWW::OpenAPIClient::Configuration->new(
    base_url => $base_url,
    access_token => ($ENV{ACYCLIC_TOKEN} // 'perl-fixture-token'),
);
my $api = WWW::OpenAPIClient::DefaultApi->new(
    WWW::OpenAPIClient::ApiClient->new($configuration),
);
my $max_uint64 = '18446744073709551615';
my ($request, $response, $hash, $operation);
if ($family eq 'actors') {
    require WWW::OpenAPIClient::Object::AcyclicActorsV1InvokeActorRequest;
    $request = WWW::OpenAPIClient::Object::AcyclicActorsV1InvokeActorRequest->new(
        actorId => 'actor-1', body => 'AQID', method => 'POST', url => 'https://example.test',
    );
    $operation = 'invoke_actor';
    $response = $api->invoke_actor(acyclic_actors_v1_invoke_actor_request => $request);
    $hash = $response->to_hash;
    die "unexpected Actors status\n" unless $hash->{status} == 200;
    die "unexpected Actors body\n" unless $hash->{body} eq 'AQID';
} elsif ($family eq 'workers') {
    require WWW::OpenAPIClient::Object::AcyclicWorkersV1InvokeDeploymentRequest;
    $request = WWW::OpenAPIClient::Object::AcyclicWorkersV1InvokeDeploymentRequest->new(
        alias => 'prod', body => 'AQID', method => 'POST', url => 'https://example.test',
    );
    $operation = 'invoke_deployment';
    $response = $api->invoke_deployment(alias => 'prod', acyclic_workers_v1_invoke_deployment_request => $request);
    $hash = $response->to_hash;
    die "unexpected Workers status\n" unless $hash->{status} == 200;
    die "unexpected Workers body\n" unless $hash->{body} eq 'AQID';
    die "Workers uint64 was not preserved\n" unless $hash->{resolvedRevision} eq $max_uint64;
} elsif ($family eq 'stream') {
    require WWW::OpenAPIClient::Object::AcyclicStreamV2ReadRequest;
    my $error_seen = 0;
    eval {
        $api->read(acyclic_stream_v2_read_request => WWW::OpenAPIClient::Object::AcyclicStreamV2ReadRequest->new(path => 'root', limit => 0));
    };
    $error_seen = 1 if $@;
    die "Stream retryable error did not raise\n" unless $error_seen;
    $request = WWW::OpenAPIClient::Object::AcyclicStreamV2ReadRequest->new(path => 'root', limit => 1);
    $operation = 'read';
    $response = $api->read(acyclic_stream_v2_read_request => $request);
    $hash = $response->to_hash;
    die "unexpected Stream record\n" unless $hash->{record}{value} eq 'AQID';
} elsif ($family eq 'objects') {
    require WWW::OpenAPIClient::Object::AcyclicObjectsV2PutObjectRequest;
    $request = WWW::OpenAPIClient::Object::AcyclicObjectsV2PutObjectRequest->new(body => 'AQID', complete => 1);
    $operation = 'put_object';
    $response = $api->put_object(acyclic_objects_v2_put_object_request => $request);
    $hash = $response->to_hash;
    die "unexpected Objects etag\n" unless $hash->{etag} eq 'etag-fixture';
    die "unexpected Objects size\n" unless $hash->{size} eq '3';
} elsif ($family eq 'inference') {
    require WWW::OpenAPIClient::Object::InferenceCustomerV1GenerateRunRequest;
    $request = WWW::OpenAPIClient::Object::InferenceCustomerV1GenerateRunRequest->new(context => 'AQID', maximumOutput => $max_uint64);
    $operation = 'runs_generate';
    $response = $api->runs_generate(inference_customer_v1_generate_run_request => $request);
    $hash = $response->to_hash;
    die "unexpected Inference run\n" unless $hash->{run}{input} eq 'AQID' && $hash->{run}{lastSequence} eq $max_uint64;
} else {
    die "unknown family: $family\n";
}

if ($family eq 'actors') {
    my $bad_configuration = WWW::OpenAPIClient::Configuration->new(base_url => $base_url, access_token => 'wrong-token');
    my $bad_api = WWW::OpenAPIClient::DefaultApi->new(WWW::OpenAPIClient::ApiClient->new($bad_configuration));
    my $error_seen = 0;
    eval { $bad_api->invoke_actor(acyclic_actors_v1_invoke_actor_request => $request); };
    $error_seen = 1 if $@;
    die "unauthenticated response did not raise an API error\n" unless $error_seen;
}

print "perl-consumer family=$family operation=$operation body=AQID uint64=$max_uint64 status=passed\n";
