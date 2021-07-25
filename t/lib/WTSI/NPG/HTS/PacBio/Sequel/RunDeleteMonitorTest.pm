package WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
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

use WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitor;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;
use WTSI::NPG::DriRODS;
use WTSI::NPG::iRODS;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

{
  package TestAPIClient;
  use Moose;

  extends 'WTSI::NPG::HTS::PacBio::Sequel::APIClient';
  override 'query_dataset_reports' => sub { my @r; return [@r]; }
}

my $pid          = $PID;
my $test_counter = 0;
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;
my $tmp_dir      = File::Temp->newdir->dirname;


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
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitor');
}

sub delete_runs : Test(14) {
  my $uri    = URI->new($server->uri . 'QueryJobs');
  my $client = TestAPIClient->new(default_interval => 10000,);
  $client->{'runs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $drirods = WTSI::NPG::DriRODS->new(environment          => \%ENV,
                                        strict_baton_version => 0);

  ## create tmp runfolder which will be deleted later
  my $run_name   = 'r54097_20170727_165601';
  my $well       = '1_A02';

  my $data_path  = catdir('t/data/pacbio/sequel', $run_name, $well);
  my $dest_coll  = catdir($irods_tmp_coll, $run_name);

  my $runfolder_path = catdir($tmp_dir,$run_name);
  mkdir $runfolder_path;
  my $runfolder_data = catdir($runfolder_path,$well);
  mkdir $runfolder_data;
  dircopy($data_path,$runfolder_data) or die $!;

  ## publish data
  my $monitor = WTSI::NPG::HTS::PacBio::Sequel::RunMonitor->new
    (api_client         => $client,
     dest_collection    => $dest_coll,
     irods              => $irods,
     local_staging_area => $tmp_dir,
     mlwh_schema        => $wh_schema);

  my ($num_jobs, $num_processed, $num_errors) =
    $monitor->publish_completed_runs;
  cmp_ok($num_jobs, '==', scalar @{$test_response},
         'Correct number of runs to publish');
  cmp_ok($num_processed, '==', $num_jobs, 'All runs processed');
  cmp_ok($num_errors, '==', 0, 'No error in any run published');

  ## delete run successfully
  my $deletable = WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitor->new
    (api_client         => $client,
     check_format       => 0,
     dest_collection    => $dest_coll,
     irods              => $drirods,
     local_staging_area => $tmp_dir,
     mlwh_schema        => $wh_schema);

  my ($dnum_runs, $dnum_processed, $dnum_deleted, $dnum_errors) = 
      $deletable->delete_runs();

  cmp_ok($dnum_runs, '==', scalar @{$test_response},
         'Correct number of runs to delete');
  cmp_ok($dnum_processed, '==', $num_jobs, 'All run folders processed');
  cmp_ok($dnum_deleted, '==', $num_jobs, 'All run folders deleted');
  cmp_ok($dnum_errors, '==', 0, 'No error in any run deleted');

  ## recopy and republish
  dircopy($data_path,$runfolder_data) or die $!;
  my ($num_jobs2, $num_processed2, $num_errors2) =
    $monitor->publish_completed_runs;

  cmp_ok($num_jobs2, '==', scalar @{$test_response},
         'Correct number of runs to publish');
  cmp_ok($num_processed2, '==', $num_jobs, 'All runs processed');
  cmp_ok($num_errors2, '==', 0, 'No error in any run published');

  ## remove a file from iRODS so run not deleted
  my $file_to_remove  = catfile($dest_coll, $well, q[m54097_170727_170646.subreads.bam]);
  $irods->remove_object($file_to_remove);

  ## fail to delete run
  my ($dnum_runs2, $dnum_processed2, $dnum_deleted2, $dnum_errors2) = 
      $deletable->delete_runs();

  cmp_ok($dnum_runs2, '==', scalar @{$test_response},
         'Correct number of runs to attempt to delete');
  cmp_ok($dnum_processed2, '==', $num_jobs -1, 'All run folders processed');
  cmp_ok($dnum_deleted2, '==', $num_jobs -1, 'Modified run not deleted');
  cmp_ok($dnum_errors2, '==', 1, 'Error in one run deletion attempt');
}

1;
