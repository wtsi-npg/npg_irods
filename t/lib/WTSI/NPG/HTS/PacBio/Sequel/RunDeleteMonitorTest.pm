package WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
use File::Spec::Functions;
use File::Which;
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
use WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;
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

my $test_response2 =
  [
   {
    reserved => 'true',
    numLRCells => 0,
    name => 'TRACTION-RUN-142',
    completedAt => '2022-06-21T11:59:08.752Z',
    chemistrySwVersion => '11.0.0.143406',
    instrumentType => 'Sequel2e',
    chipType => '8mChip',
    instrumentName => '64089E',
    context => 'r64089e_20220615_171559',
    instrumentSwVersion => '11.0.0.144466',
    numCellsCompleted => 4,
    totalCells => 4,
    primaryAnalysisSwVersion => '11.0.0.144466',
    status => 'Complete',
    numStandardCells => 4,
    createdAt => '2022-06-15T17:13:54.580Z',
    startedAt => '2022-06-15T17:23:36.934Z',
    createdBy => 'unknown',
    numCellsFailed => 0,
    instrumentSerialNumber => '64089e',
    transfersCompletedAt => '2022-06-21T17:40:54.700Z',
    uniqueId => '909d36e5-6385-4c2a-8886-72483eb6e31f',
    ccsExecutionMode => 'OnInstrument',
    summary => 'TRAC-2-605 130pM TRAC-2-556 130pM TRAC-2-609 120pM TRAC-2-610 130pM'
   }
  ];

my $test_response3 =
  [
   {
    reserved => 'true',
    numLRCells => 0,
    name => 'TRACTION-RUN-282',
    completedAt => '2022-10-05T06:42:30.786Z',
    chemistrySwVersion => '11.0.0.143406',
    instrumentType => 'Sequel2e',
    chipType => '8mChip',
    instrumentName => '64089E',
    context => 'r64089e_20220930_164018',
    instrumentSwVersion => '11.0.0.144466',
    numCellsCompleted => 4,
    totalCells => 4,
    primaryAnalysisSwVersion => '11.0.0.144466',
    status => 'Complete',
    numStandardCells => 4,
    createdAt => '2022-09-30T16:39:35.372Z',
    startedAt => '2022-09-30T16:42:32.351Z',
    createdBy => 'unknown',
    numCellsFailed => 0,
    instrumentSerialNumber => '64089e',
    transfersCompletedAt => '2022-10-05T14:57:20.745Z',
    uniqueId => 'be3bc59f-c22c-4ade-8419-a839e86e181c',
    ccsExecutionMode => 'OnInstrument',
    summary  => 'TRAC-2-1515 130pM TRAC-2-1516 130pM TRAC-2-1504 120pM TRAC-2-1506 90pM'
   }
  ];


my $server = Test::HTTP::Server->new;

# Handler for /QueryJobs
sub Test::HTTP::Server::Request::QueryJobs {
  my ($self) = @_;
  return to_json($test_response);
}

sub Test::HTTP::Server::Request::QueryJobs2 {
  my ($self) = @_;
  return to_json($test_response2);
}

sub Test::HTTP::Server::Request::QueryJobs3 {
  my ($self) = @_;
  return to_json($test_response3);
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

sub script: Test(1) {
  isnt(which("generate_pac_bio_id"), undef, "id generation script installed");
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
  chmod (0770, $runfolder_data) or die "Chmod 0770 directory $runfolder_data failed : $!";

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
  chmod (0770, $runfolder_data) or die "Chmod 0770 directory $runfolder_data failed : $!";
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


sub delete_run_on_board_deplexing : Test(7) {
## on instrument deplexing: 1 cell   

  my $uri    = URI->new($server->uri . 'QueryJobs2');
  my $client = TestAPIClient->new(default_interval => 10000,);
  $client->{'runs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $drirods = WTSI::NPG::DriRODS->new(environment          => \%ENV,
                                        strict_baton_version => 0);

  ## create tmp runfolder which will be deleted later
  my $run_name   = 'r64089e_20220615_171559';
  my $well       = '1_A01';

  my $data_path  = catdir('t/data/pacbio/sequel', $run_name, $well);
  my $dest_coll  = catdir($irods_tmp_coll, $run_name);

  my $runfolder_path = catdir($tmp_dir,$run_name);
  mkdir $runfolder_path;
  my $runfolder_data = catdir($runfolder_path,$well);
  mkdir $runfolder_data;
  dircopy($data_path,$runfolder_data) or die $!;
  chmod (0770, $runfolder_data) or die "Chmod 0770 directory $runfolder_data failed : $!";

  ## publish data
  my $monitor = WTSI::NPG::HTS::PacBio::Sequel::RunMonitor->new
    (api_client         => $client,
     dest_collection    => $dest_coll,
     irods              => $irods,
     local_staging_area => $tmp_dir,
     mlwh_schema        => $wh_schema);

  my ($num_jobs, $num_processed, $num_errors) =
    $monitor->publish_completed_runs;
  cmp_ok($num_jobs, '==', scalar @{$test_response2},
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
}

sub delete_run_on_board_deplexing_fail : Test(6) {
## on instrument deplexing: 2 cells - 1 cell (A1) failed to deplex

  my $uri    = URI->new($server->uri . 'QueryJobs3');
  my $client = TestAPIClient->new(default_interval => 10000,);
  $client->{'runs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $drirods = WTSI::NPG::DriRODS->new(environment          => \%ENV,
                                        strict_baton_version => 0);

  ## create tmp runfolder which will be deleted later
  my $run_name   = 'r64089e_20220930_164018';
  my $data_path  = catdir('t/data/pacbio/sequel', $run_name);

  my $dest_coll  = catdir($irods_tmp_coll, $run_name);
  my $runfolder_path = catdir($tmp_dir,$run_name);
  mkdir $runfolder_path;
  
  dircopy($data_path,$runfolder_path) or die $!;
  chmod (0770, "$runfolder_path/1_A01") or die "Chmod 0770 directory failed : $!";  
  chmod (0770, "$runfolder_path/2_B01") or die "Chmod 0770 directory failed : $!";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $data_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  
  cmp_ok($num_processed, '==', 18, '18/22 files processed and uploaded');
  cmp_ok($num_errors,    '==', 1, 'One error in one run published');

  ## attempt to delete run
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
  cmp_ok($dnum_processed, '==', 0, 'No run folders successfully processed');
  cmp_ok($dnum_deleted, '==', 0, 'No run folders deleted');
  cmp_ok($dnum_errors, '==', 1, 'One error so no runs deleted'); 
}

1;
