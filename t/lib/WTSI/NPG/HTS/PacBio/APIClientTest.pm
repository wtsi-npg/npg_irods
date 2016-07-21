package WTSI::NPG::HTS::PacBio::APIClientTest;

use strict;
use warnings;

use JSON;
use Log::Log4perl;
use Test::HTTP::Server;
use Test::More;
use Test::Exception;
use URI;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::PacBio::APIClient;

my $test_response =
  [
   {
    CollectionID           => 1000,
    CollectionNumber       => 1,
    CollectionOrderPerWell => 1,
    CollectionState        => 'Complete',
    IndexOfLook            => 1,
    IndexOfMovie           => 1,
    IndexOfStrobe          => 0,
    JobStatus              => 'Complete',
    JobType                => 'PacBio.Instrument.Jobs.PrimaryAnalysisJob',
    OutputFilePath         => 'pbids://localhost/superfoo/12345_678/A01_1',
    Plate                  => 9999,
    ResolvedPlatformUri    => 'pbids://localhost/superfoo/12345_678/A01_1',
    RunType                => 'SEQUENCING',
    TotalMoviesExpected    => 1,
    TotalStrobesExpected   => 0,
    Well                   => 'A01',
    WhenModified           => '2016-05-20T23:09:10Z',
   },
   {
    CollectionID           => 10869,
    CollectionNumber       => 2,
    CollectionOrderPerWell => 1,
    CollectionState        => 'Complete',
    IndexOfLook            => 1,
    IndexOfMovie           => 1,
    IndexOfStrobe          => 0,
    JobStatus              => 'Complete',
    JobType                => 'PacBio.Instrument.Jobs.PrimaryAnalysisJob',
    OutputFilePath         => 'pbids://localhost/superfoo/12345_678/B01_1',
    Plate                  => 46514,
    ResolvedPlatformUri    => 'pbids://localhost/superfoo/12345_678/B01_1',
    RunType                => 'SEQUENCING',
    TotalMoviesExpected    => 1,
    TotalStrobesExpected   => 0,
    Well                   => 'B01',
    WhenModified           => '2016-05-21T02:47:24Z',
   }
  ];

my $server = Test::HTTP::Server->new;

# Handler for /QueryJobs
sub Test::HTTP::Server::Request::QueryJobs {
  my ($self) = @_;

  return to_json($test_response);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::APIClient');
}

sub query_jobs : Test(1) {
  my $uri = URI->new($server->uri . 'QueryJobs');
  my $client = WTSI::NPG::HTS::PacBio::APIClient->new(api_uri => $uri);
  is_deeply($client->query_jobs, $test_response);
}

1;
