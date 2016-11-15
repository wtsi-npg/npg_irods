package WTSI::NPG::HTS::PacBio::MetaUpdaterTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::PacBio::MetaUpdater;
use WTSI::NPG::iRODS;


Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/pacbio_meta_updater';
my $fixture_path = "./t/fixtures";

my $db_dir = File::Temp->newdir;
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
    $irods->add_collection("PacBioMetaUpdaterTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::MetaUpdater');
}





1;
