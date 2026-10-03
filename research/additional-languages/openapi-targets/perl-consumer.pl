use strict;
use warnings;
use JSON qw(encode_json);
use WWW::OpenAPIClient::ApiClient;
use WWW::OpenAPIClient::Configuration;
use WWW::OpenAPIClient::DefaultApi;
use WWW::OpenAPIClient::Object::AcyclicActorsV1InvokeActorRequest;

my $base_url = ($ENV{ACYCLIC_BASE_URL} // 'http://127.0.0.1:18766');
my $configuration = WWW::OpenAPIClient::Configuration->new(
    base_url => $base_url,
    access_token => ($ENV{ACYCLIC_TOKEN} // 'perl-fixture-token'),
);
my $api = WWW::OpenAPIClient::DefaultApi->new(
    WWW::OpenAPIClient::ApiClient->new($configuration),
);
my $request = WWW::OpenAPIClient::Object::AcyclicActorsV1InvokeActorRequest->new(
    actorId => 'actor-1',
    body => 'AQID',
    method => 'POST',
    url => 'https://example.test',
);
my $request_json = encode_json($request->to_hash);
die "request bytes were not canonical base64: $request_json\n"
    unless $request_json =~ /"body":"AQID"/;
my $max_uint64 = '18446744073709551615';
my $max_uint64_json = encode_json({ configurationRevision => $max_uint64 });
die "uint64 decimal-string fixture was not preserved: $max_uint64_json\n"
    unless $max_uint64_json =~ /"configurationRevision":"$max_uint64"/;
my $response = $api->invoke_actor(
    acyclic_actors_v1_invoke_actor_request => $request,
);
my $hash = $response->to_hash;
die "unexpected status\n" unless $hash->{status} == 200;
die "unexpected response body\n" unless $hash->{body} eq 'AQID';

my $bad_configuration = WWW::OpenAPIClient::Configuration->new(
    base_url => $base_url,
    access_token => 'wrong-token',
);
my $bad_api = WWW::OpenAPIClient::DefaultApi->new(
    WWW::OpenAPIClient::ApiClient->new($bad_configuration),
);
my $error_seen = 0;
eval {
    $bad_api->invoke_actor(
        acyclic_actors_v1_invoke_actor_request => $request,
    );
};
$error_seen = 1 if $@;
die "unauthenticated response did not raise an API error\n" unless $error_seen;

print "perl-consumer status=$hash->{status} request_body=AQID response_body=$hash->{body} uint64=$max_uint64 unauthenticated=raised\n";
