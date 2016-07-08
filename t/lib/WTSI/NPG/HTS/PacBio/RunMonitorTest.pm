package WTSI::NPG::HTS::PacBio::RunMonitorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use JSON;
use Log::Log4perl;
use Test::HTTP::Server;
use Test::More;
use Test::Exception;
use URI;

use base qw[WTSI::NPG::HTS::Test];

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

use WTSI::NPG::HTS::PacBio::APIClient;
use WTSI::NPG::HTS::PacBio::RunMonitor;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

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
    OutputFilePath         => 'pbids://localhost/superfoo/45137_1095/A01_1',
    Plate                  => 9999,
    ResolvedPlatformUri    => 'pbids://localhost/superfoo/45137_1095/A01_1',
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
    OutputFilePath         => 'pbids://localhost/superfoo/45137_1095/B01_1',
    Plate                  => 46514,
    ResolvedPlatformUri    => 'pbids://localhost/superfoo/451317_1095/B01_1',
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

my $wh_schema;

my $irods_tmp_coll;

sub setup_databases : Test(startup) {
  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  $wh_schema = TestDB->new(sqlite_utf8_enabled => 1,
                           verbose             => 0)->create_test_db
    ('WTSI::DNAP::Warehouse::Schema', "$fixture_path/ml_warehouse",
     $wh_db_file);
}

sub teardown_databases : Test(shutdown) {
  $wh_schema->storage->disconnect;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("RunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::APIClient');
}

sub publish_completed_runs : Test(3) {
  my $uri    = URI->new($server->uri . 'QueryJobs');
  my $client = WTSI::NPG::HTS::PacBio::APIClient->new(api_uri => $uri);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $dest_coll = $irods_tmp_coll;

  my $monitor = WTSI::NPG::HTS::PacBio::RunMonitor->new
    (api_client         => $client,
     dest_collection    => $dest_coll,
     path_uri_filter    => '127.0.0.1',
     irods              => $irods,
     local_staging_area => $data_path,
     mlwh_schema        => $wh_schema);

  my ($num_jobs, $num_processed, $num_errors) =
    $monitor->publish_completed_runs;

  cmp_ok($num_jobs, '==', scalar @{$test_response}, 'Correct number of jobs');
  cmp_ok($num_processed, '==', $num_jobs, 'All jobs processed');
  cmp_ok($num_errors, '==', 0, 'No error in any job');
}

1;
