package WTSI::NPG::HTS::ONT::GridIONMetaUpdaterTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::HTS::ONT::GridIONMetaUpdater;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/gridion_meta_updater';
my $fixture_path = "./t/fixtures";

my $db_dir = File::Temp->newdir;
my $wh_schema;

my $f5_data_file = 'GA10000_fast5_2017-10-18T161023.0.tar';
my $fq_data_file = 'GA10000_fastq_2017-10-18T161024.0.tar';
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
    $irods->add_collection("GridIONMetaUpdaterTest.$pid.$test_counter");
  $test_counter++;

  foreach my $file ($f5_data_file, $fq_data_file) {
    $irods->add_object("$data_path/$file", "$irods_tmp_coll/$file");
    $irods->add_object_avu("$irods_tmp_coll/$file", 'experiment_name', 2);
    $irods->add_object_avu("$irods_tmp_coll/$file", 'device_id', 'GA10000');
  }
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub update_secondary_metadata : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $updater = WTSI::NPG::HTS::ONT::GridIONMetaUpdater->new
    (irods       => $irods,
     mlwh_schema => $wh_schema);

  my @paths_to_update = ("$irods_tmp_coll/$f5_data_file",
                         "$irods_tmp_coll/$fq_data_file");
  cmp_ok($updater->update_secondary_metadata(\@paths_to_update),
         '==', scalar @paths_to_update,
         'All iRODS paths processed without errors');

  my $expected_meta =
    [
     {attribute => $GRIDION_DEVICE_ID,       value => 'GA10000'},
     {attribute => $EXPERIMENT_NAME,         value => '2'},
     {attribute => $SAMPLE_NAME,             value => '4944STDY7082749'},
     {attribute => $SAMPLE_DONOR_ID,         value => '4944STDY7082749'},
     {attribute => $SAMPLE_ID,               value => 3302237},
     {attribute => $SAMPLE_SUPPLIER_NAME,    value => 'Lambda1'},
     {attribute => $STUDY_NAME,              value => 'GridION test study'},
     {attribute => $STUDY_ID,                value => 4944},
     {attribute => $STUDY_TITLE,             value => 'GridION test study'},
    ];

  foreach my $file ($f5_data_file, $fq_data_file) {
    my $obj = WTSI::NPG::HTS::DataObject->new
      (collection  => $irods_tmp_coll,
       data_object => $file,
       irods       => $irods);

    is_deeply($obj->metadata, $expected_meta,
            'Secondary metadata updated correctly') or
              diag explain $obj->metadata;
  }
}

1;
