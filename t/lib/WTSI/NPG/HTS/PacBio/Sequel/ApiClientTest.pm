package WTSI::NPG::HTS::PacBio::Sequel::ApiClientTest;

use strict;
use warnings;

use JSON;
use Log::Log4perl;
use Test::More;
use Test::Exception;
use Test::LWP::UserAgent;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::Sequel::APIClient;

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

my $user_agent;
sub setup_useragent : Test(startup) {
    $user_agent = Test::LWP::UserAgent->new(network_fallback => 1);

    my $test_response_c = [{name => "DN779746P-C1",completedAt => "2021-03-13T03=>18=>46.799Z",instrumentName=>"64230E",context=>"m64230e_210311_172319",multiJobId=>0,well=>"A01",status=>"Complete",importedAt=>"2021-03-13T03=>19=>19.296Z",instrumentId=>"64230e",startedAt=>"2021-03-11T17=>23=>19.344Z",cellType=>"Standard",uniqueId=>"5933186c-d1b2-4aaf-832c-977dce603509",ccsExecutionMode=>"OffInstrument",runId=>"b3863f97-7044-4b82-a1dd-d1960877ae63",ccsId=>"83fb96d8-5e84-4be2-84ff-d725984f88ec",movieMinutes=>1440.0}];

    $user_agent->map_response(
      qr{http://localhost:8071/smrt-link/runs/b3863f97-7044-4b82-a1dd-d1960877ae63/collections},
      HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], encode_json($test_response_c)));

    my $test_response_r = {uniqueId => 'b3863f97-7044-4b82-a1dd-d1960877ae63',ccsExecutionMode => 'OffInstrument'};

    $user_agent->map_response(
      qr{http://localhost:8071/smrt-link/runs/b3863f97-7044-4b82-a1dd-d1960877ae63},
      HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], encode_json($test_response_r)));
}

sub test_query_run_collections : Test(2) {

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my $run_id = q[b3863f97-7044-4b82-a1dd-d1960877ae63];
  my $wells  = $client->query_run_collections($run_id);

  ok(scalar @{$wells} == 1,  q{query_run_collections returned 1 collection});
  ok($wells->[0]->{runId} eq $run_id, q{query_run_collections returned expected run id});
}

1;
