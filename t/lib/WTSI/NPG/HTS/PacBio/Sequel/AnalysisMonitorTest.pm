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
  comment => "8pM",
  createdAt => "2018-04-23T09:04:08.042Z",
  createdBy => "chc",
  id => 1612,
  jobTypeId => "pbsmrtpipe",
  jsonSettings => "{\"name\":\"Demultiplexing of DN505769FNCTCPool4\",\"entryPoints\":[{\"entryId\":\"eid_subread\",\"fileTypeId\":\"PacBio.DataSet.SubreadSet\",\"datasetId\":\"07d85801-6a09-4728-982c-e3c048f95bd8\"},{\"entryId\":\"eid_barcode\",\"fileTypeId\":\"PacBio.DataSet.BarcodeSet\",\"datasetId\":\"e8eadc61-ffcc-9170-68cb-99539f8c82d3\"}],\"description\":\"8pM\",\"projectId\":1,\"workflowOptions\":[],\"taskOptions\":[{\"id\":\"lima.task_options.peek_guess_tc\",\"value\":true,\"optionTypeId\":\"boolean\"},{\"id\":\"lima.task_options.library_same_tc\",\"value\":true,\"optionTypeId\":\"boolean\"},{\"id\":\"lima.task_options.minscore\",\"value\":0,\"optionTypeId\":\"integer\"}],\"pipelineId\":\"pbsmrtpipe.pipelines.sa3_ds_barcode2\"}",
  name => "Demultiplexing of DN505769FNCTCPool4",
  parentMultiJobId => 1601,
  path => "t/data/pacbio/sequel_analysis/001612",
  projectId => 1,
  smrtlinkVersion => "5.1.0.26412",
  state => "SUCCESSFUL",
  updatedAt => "2018-04-24T11:02:04.223Z",
  uuid => "3bf09718-58fe-4fb6-a32a-9adbda384b27",
  workflow => "{}",
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
     mlwh_schema        => $wh_schema);

  my ($num_jobs, $num_processed, $num_errors) =
    $monitor->publish_analysed_cells;

  cmp_ok($num_jobs, '==', scalar @{$test_response}, 'Correct number of jobs');
  cmp_ok($num_processed, '==', $num_jobs, 'All jobs processed');
  cmp_ok($num_errors, '==', 0, 'No error in any job');
}

1;
