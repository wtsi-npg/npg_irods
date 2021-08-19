package WTSI::NPG::HTS::PacBio::Sequel::RunAuditorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
use File::Path qw/make_path/;
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

use WTSI::NPG::HTS::PacBio::Sequel::RunAuditor;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;
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
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::RunAuditor');
}

sub audit_runs : Test(20) {
  my $uri    = URI->new($server->uri . 'QueryJobs');
  my $client = TestAPIClient->new(default_interval => 10000,);
  $client->{'runs_api_uri'} = $uri;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
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

  chmod (0770, $runfolder_path) or die "Chmod 0770 directory $runfolder_path failed : $!";
  chmod (0770, $runfolder_data) or die "Chmod 0770 directory $runfolder_data failed : $!";
 
  my @init_args = 
    (api_client         => $client,
     check_format       => 1,
     dest_collection    => $dest_coll,
     irods              => $irods,
     local_staging_area => $tmp_dir,
     mlwh_schema        => $wh_schema);
 
  my @c_init_args = (@init_args, check_format => 1, dry_run => 1);
  my @n_init_args = (@init_args, check_format => 0, dry_run => 1);
  my @d_init_args = (@init_args, check_format => 0, dry_run => 0);

  my $auditor1 = WTSI::NPG::HTS::PacBio::Sequel::RunAuditor->new(@c_init_args);

  throws_ok { $auditor1->valid_runfolder_format($runfolder_path)} qr/Folder failed format checks/,
    'Expected runfolder path format error found';

  my $valid1B = $auditor1->valid_runfolder_format(q[/YYY/XXX/pacbio/r54097_20170727_165601]);
  cmp_ok($valid1B , '==', 1, 'No expected runfolder path format error');

  my $valid1C = $auditor1->valid_runfolder_format(q[/YYY/XXX/pacbio/staging/r54097_20170727_165601]);
  cmp_ok($valid1C , '==', 1, 'No expected runfolder path format error');

  my $valid2A = $auditor1->valid_runfolder_directory($runfolder_path);
  cmp_ok($valid2A, '==', 1, 'No expected runfolder directory error');

  ## folder format check, no dry run
  my ($num_run1, $num_processed1, $num_actioned1, $num_errors1) =
    $auditor1->check_runs;

  cmp_ok($num_run1, '==', scalar @{$test_response},
         'Correct number of runs to check - check_format => 1, dry_run => 1');
  cmp_ok($num_processed1, '==', $num_run1 -1, 'No runs processed');
  cmp_ok($num_actioned1, '==', $num_run1 -1, 'No runs actioned');
  cmp_ok($num_errors1, '==', 1, 'Expected error found [folder path format]');

  ## no folder format check, dry run
  my $auditor2 = WTSI::NPG::HTS::PacBio::Sequel::RunAuditor->new(@n_init_args);
  my ($num_run2, $num_processed2, $num_actioned2, $num_errors2) =
    $auditor2->check_runs;

  cmp_ok($num_run2, '==', scalar @{$test_response},
         'Correct number of runs to check - check_format => 0, dry_run => 1');
  cmp_ok($num_processed2, '==', $num_run2, 'All runs processed');
  cmp_ok($num_actioned2, '==', $num_run2 -1, 'No runs actioned');
  cmp_ok($num_errors2, '==', 0, 'No errors found [folder path format]');

  ## no folder format check, no dry run, permissions default
  my $auditor3 = WTSI::NPG::HTS::PacBio::Sequel::RunAuditor->new(@d_init_args);
  my ($num_run3, $num_processed3, $num_actioned3, $num_errors3) =
    $auditor3->check_runs;

  cmp_ok($num_run3, '==', scalar @{$test_response},
         'Correct number of runs to check - check_format => 0, dry_run => 0');
  cmp_ok($num_processed3, '==', $num_run3, 'All runs processed');
  cmp_ok($num_actioned3, '==', $num_run3 -1, 'No runs actioned');
  cmp_ok($num_errors3, '==', 0, 'No errors found');

  ## chmod 700 directory paths
  chmod (0700, $runfolder_path) or die "Chmod 0700 directory $runfolder_path failed : $!";
  chmod (0700, $runfolder_data) or die "Chmod 0700 directory $runfolder_data failed : $!";
  
  my ($num_run4, $num_processed4, $num_actioned4, $num_errors4) =
    $auditor3->check_runs;

  ## no folder format check, no dry run, permissions set to user read only
  cmp_ok($num_run4, '==', scalar @{$test_response},
         'Correct number of runs to check - check_format => 0, dry_run => 0');
  cmp_ok($num_processed4, '==', $num_run4, 'All runs processed');
  cmp_ok($num_actioned4, '==', $num_run4, 'All runs actioned to correct permissions');
  cmp_ok($num_errors4, '==', 0, 'No errors found');

}

1;
