package WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitorTest;

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
use WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $test_response =
  [
   {
    "subJobTypeId" => "cromwell.workflows.pb_demux_ccs",
    "name" => "TRAC-2-8046-Cell1_redo_demultiplex",
    "updatedAt" => "2024-04-08T16:39:32.434Z",
    "workflow" => "{}",
    "path" => "t/data/pacbio/sequel_analysis/0000019480",
    "state" => "SUCCESSFUL",
    "tags" => "",
    "uuid" => "41b9c9b0-5621-41d8-a373-e04a641ddb2b",
    "externalJobId" => "2a15bfd3-d378-4701-9dea-83d77c67a062",
    "jobStartedAt" => "2024-04-08T15:45:35.893Z",
    "applicationName" => "Demultiplex Barcodes",
    "projectId" => 1,
    "childJobsCount" => 0,
    "jobCompletedAt" => "2024-04-08T16:39:32.434Z",
    "jobTypeId" => "analysis",
    "id" => 19480,
    "smrtlinkVersion" => "13.0.0.207600",
    "comment" => "Description for job Run Analysis Application",
    "isNested" => "false",
    "createdAt" => "2024-04-08T15:45:34.218Z",
    "isActive" => "true",
    "isMultiJob" => "false",
    "jsonSettings" =>  "",
    "jobUpdatedAt" => "2024-04-08T16:39:32.434Z"
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
    $irods->add_collection("AnalysisPublisherTest.$pid.$test_counter");
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


sub publish_completed_jobs : Test(3) {
  my $uri    = URI->new($server->uri . 'QueryJobs');
  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new
     (default_interval => 10000,);
  $client->{'jobs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $dest_coll = $irods_tmp_coll;

  my $monitor = WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor->new
    (api_client         => $client,
     dest_collection    => $dest_coll,
     irods              => $irods,
     mlwh_schema        => $wh_schema,
     pipeline_name      => 'cromwell.workflows.pb_demux_ccs',
     task_name          => 'call-lima/execution',
     );

  my ($num_jobs, $num_processed, $num_errors) =
    $monitor->publish_analysed_cells;

  cmp_ok($num_jobs, '==', scalar @{$test_response}, 'Correct number of jobs');
  cmp_ok($num_processed, '==', $num_jobs, 'All jobs processed');
  cmp_ok($num_errors, '==', 0, 'No error in any job');
}

1;
