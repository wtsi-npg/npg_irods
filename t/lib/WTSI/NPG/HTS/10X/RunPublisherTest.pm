package WTSI::NPG::HTS::10X::RunPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions qw[catfile];
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::10X::RunPublisher;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $wh_schema;
my $lims_factory;

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
  # $irods->remove_collection($irods_tmp_coll);
}

sub wombat : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $source_path = "$data_path/10X/cellranger";

  my $dest_coll    = "$irods_tmp_coll/wombat";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::10X::RunPublisher->new
    (dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $source_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  cmp_ok($num_errors, '==', 0, 'No errors on publishing');
}

1;
