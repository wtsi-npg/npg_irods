package WTSI::NPG::HTS::PacBio::MetaUpdaterTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::HTS::PacBio::MetaUpdater;
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
my $data_path    = './t/data/pacbio_meta_updater';
my $fixture_path = "./t/fixtures";

my $db_dir = File::Temp->newdir;
my $wh_schema;

my $data_file = 'm131209_183112_00127_c100579142550000001823092301191430_s1_p0.1.bax.h5';
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

  $irods->add_object("$data_path/$data_file", "$irods_tmp_coll/$data_file");
  $irods->add_object_avu("$irods_tmp_coll/$data_file", 'run', 45137);
  $irods->add_object_avu("$irods_tmp_coll/$data_file", 'well', 'A01');
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::MetaUpdater');
}

sub update_secondary_metadata : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $updater = WTSI::NPG::HTS::PacBio::MetaUpdater->new
    (irods       => $irods,
     mlwh_schema => $wh_schema);

  my @paths_to_update = ("$irods_tmp_coll/$data_file");

  cmp_ok($updater->update_secondary_metadata(\@paths_to_update),
         '==', scalar @paths_to_update,
         'All iRODS paths processed without errors');
  my $expected_meta =
    [
     {attribute => $LIBRARY_ID,              value => 15977171},
     {attribute => $PACBIO_LIBRARY_NAME,     value => 'DN434306G-A1'},
     {attribute => $PACBIO_RUN,              value     => 45137},
     {attribute => $SAMPLE_NAME,             value => '2572STDY6358500'},
     {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS1075968'},
     {attribute => $SAMPLE_COMMON_NAME,      value => 'Anopheles gambiae'},
     {attribute => $SAMPLE_DONOR_ID,         value => '2572STDY6358500'},
     {attribute => $SAMPLE_ID,               value => 2567488},
     {attribute => $SAMPLE_PUBLIC_NAME,      value => 70628},
     {attribute => $SAMPLE_SUPPLIER_NAME,    value => 'AR0091-CW'},
     {attribute => $STUDY_NAME,              value => 'Ag 1000g'},
     {attribute => $STUDY_ACCESSION_NUMBER,  value => 'ERP002372'},
     {attribute => $STUDY_ID,                value => 2572},
     {attribute => $STUDY_TITLE,
      value     => 'Anopheles Genome Variation Project'},
     {attribute => $PACBIO_WELL,             value     => 'A01'}
    ];

  my $obj = WTSI::NPG::HTS::DataObject->new
    (collection  => $irods_tmp_coll,
     data_object => $data_file,
     irods       => $irods);
  is_deeply($obj->metadata, $expected_meta,
            'Secondary metadata updated correctly') or
              diag explain $obj->metadata;
}

1;
