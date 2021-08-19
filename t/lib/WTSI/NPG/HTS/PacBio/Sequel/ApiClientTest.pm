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

my $client;
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

    my $test_response_d = [{comments => "ccs dataset converted",datasetType => "PacBio.DataSet.ConsensusReadSet",runName => "82553",numResources => 1,numChildren => 0,metadataContextId => "m64094e_210521_211114",parentUuid => "b1b358f0-44c6-482e-86fe-0f09d83888d9",cellIndex => 1,wellName => "B01",createdBy => "bp7",isActive => 1,createdAt => "2021-05-23T14:41:20.679Z",jobId => 3726,importedAt => "2021-05-23T16:16:58.174Z",md5 => "ab7d81e2cfb1ad7c61b0d0bfee732e5d",dnaBarcodeName => "bc1017_BAK8B_OA--bc1017_BAK8B_OA",uuid => "4b399e7b-4f12-4332-9ea2-be39a8dcd461",instrumentName => "64094E",tags => "barcoded,ccs",instrumentControlVersion => "10.1.0.119549",updatedAt => "2021-05-23T14:41:20.679Z",name => "DN804974W-B1-Cell2 (CCS) (copy) (DN804974W-B1 )",totalLength => 2536548591,projectId => 1,numRecords => 214383,wellSampleName => "DN804974W-B1",bioSampleName => "DN804974W-B1",version => "3.0.1",cellId => "DA079780",id => 6558}];

    $user_agent->map_response(
      qr{http://localhost:8071/smrt-link/datasets/ccsreads},
      HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], encode_json($test_response_d)));

  $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);
}

sub test_query_run_collections : Test(2) {

  my $run_id = q[b3863f97-7044-4b82-a1dd-d1960877ae63];
  my $wells  = $client->query_run_collections($run_id);

  ok(scalar @{$wells} == 1,  q{query_run_collections returned 1 collection});
  ok($wells->[0]->{runId} eq $run_id, q{query_run_collections returned expected run id});
}

sub test_query_datasets : Test(3) {

  my $datasets = $client->query_datasets(q[ccsreads]);

  ok(scalar @{$datasets} == 1,  q{query_datasets returned 1 dataset});
  ok($datasets->[0]->{datasetType} eq q[PacBio.DataSet.ConsensusReadSet], 
    q{query_datasets returned expected dataset type});

  throws_ok { $client->query_datasets() } qr/A defined dataset_type is required/,
    q{query_datasets throws when no type is given};
}

1;
