package WTSI::NPG::HTS::PacBio::Sequel::RunMonitorTest;

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

use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/sequel';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $test_response =
  [
   {
   reserved => 'true',
   name => 55639,
   completedAt => '2017-07-27T16:36:25.087Z',
   instrumentName => 'SQ54097',
   context => 'r54097_20170727_165601',
   instrumentSwVersion => '5.0.0.6235',
   numCellsCompleted => 1,
   totalCells => 1,
   primaryAnalysisSwVersion => '5.0.0.6236',
   status => 'Complete',
   createdAt => '2017-07-27T12:39:11.108Z',
   startedAt => '2017-07-27T13:36:25.087Z',
   createdBy => 'unknown',
   numCellsFailed => 0,
   instrumentSerialNumber => 54097,
   transfersCompletedAt => '2017-07-27T17:36:25.087Z',
   uniqueId => 'ecbf020d-4437-48d8-9d00-935142094728',
   summary => 'initial titration 7pM'
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
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::APIClient');
}


sub publish_completed_runs : Test(3) {
  my $uri    = URI->new($server->uri . 'QueryJobs');
  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new
     (default_interval => 10000,);
  $client->{'runs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $dest_coll = $irods_tmp_coll;

  my $monitor = WTSI::NPG::HTS::PacBio::Sequel::RunMonitor->new
    (api_client         => $client,
     dest_collection    => $dest_coll,
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
