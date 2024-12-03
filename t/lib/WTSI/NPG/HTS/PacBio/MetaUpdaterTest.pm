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

  $irods->add_object("$data_path/$data_file",
                     "$irods_tmp_coll/$data_file",
                     $WTSI::NPG::iRODS::CALC_CHECKSUM);
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

sub update_secondary_metadata : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $updater = WTSI::NPG::HTS::PacBio::MetaUpdater->new
    (irods       => $irods,
     mlwh_schema => $wh_schema);

  my $run_name = '45137';
  my $well_label = 'A1';
  my $study_id = 2572;
  my $study_name = 'Ag 1000g';

  # Tests for a case when the plate number is not defined.
  my $row = $wh_schema->resultset('PacBioRun')->search({
    pac_bio_run_name => $run_name, well_label => $well_label})->next();
  is ($row->plate_number, undef, 'Plate number is undefined');
  my $study_row = $wh_schema->resultset('Study')->search(
        {id_study_lims => $study_id})->next();
  is($study_row->name, $study_name, 'Current study name value');

  my @paths_to_update = ("$irods_tmp_coll/$data_file");

  cmp_ok($updater->update_secondary_metadata(\@paths_to_update),
         '==', scalar @paths_to_update,
         'All iRODS paths processed without errors');

  my $expected_meta =
    [
     {attribute => $LIBRARY_ID,              value => 15977171},
     {attribute => $PACBIO_LIBRARY_NAME,     value => 'DN434306G-A1'},
     {attribute => $PACBIO_RUN,              value => $run_name},
     {attribute => $SAMPLE_NAME,             value => '2572STDY6358500'},
     {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS1075968'},
     {attribute => $SAMPLE_COMMON_NAME,      value => 'Anopheles gambiae'},
     {attribute => $SAMPLE_DONOR_ID,         value => '2572STDY6358500'},
     {attribute => $SAMPLE_ID,               value => 2567488},
     {attribute => $SAMPLE_LIMS,             value => 'SQSCP'},
     {attribute => $SAMPLE_PUBLIC_NAME,      value => 70628},
     {attribute => $SAMPLE_SUPPLIER_NAME,    value => 'AR0091-CW'},
     {attribute => $SAMPLE_UUID,             value => '1ca1a900-e463-11e5-88b1-3c4a9275d6c6'},
     {attribute => $STUDY_NAME,              value => $study_name},
     {attribute => $STUDY_ACCESSION_NUMBER,  value => 'ERP002372'},
     {attribute => $STUDY_ID,                value => $study_id},
     {attribute => $PACBIO_STUDY_NAME,       value => $study_name},
     {attribute => $STUDY_TITLE,
      value     => 'Anopheles Genome Variation Project'},
     {attribute => $PACBIO_WELL,             value     => 'A01'},
    ];

  my $obj = WTSI::NPG::HTS::DataObject->new
    (collection  => $irods_tmp_coll,
     data_object => $data_file,
     irods       => $irods);
  is_deeply($obj->metadata, $expected_meta,
            'Secondary metadata updated correctly') or
              diag explain $obj->metadata;

  # Tests for a case when the plate number is defined.
  my $plate_number = 2;
  $row->update({'plate_number' => $plate_number}); # Update plate number in mlwh
  $irods->add_object_avu("$irods_tmp_coll/$data_file",
    'plate_number', $plate_number); # Update primary metadata
  my $new_study_name = 'Updated for the test';
  $study_row->update({name => $new_study_name}); # Update study name in mlwh
  # Create a linked product row with QC outcome defined.
  my $rw_row = $wh_schema->resultset('PacBioRunWellMetric')->create({
    pac_bio_run_name => $run_name,
    well_label => $well_label,
    plate_number => $plate_number,
    id_pac_bio_product => 'A' x 64,
    instrument_type => 'SomeType'
  });
  my $p_row = $wh_schema->resultset('PacBioProductMetric')->create({
    id_pac_bio_rw_metrics_tmp => $rw_row->id_pac_bio_rw_metrics_tmp,
    id_pac_bio_tmp => $row->id_pac_bio_tmp,
    id_pac_bio_product => $rw_row->id_pac_bio_product
  });

  # Call the updater again.
  $updater->update_secondary_metadata(\@paths_to_update);
  
  # Inspect the metadata after the update.
  my $updated = WTSI::NPG::HTS::DataObject->new
    (collection  => $irods_tmp_coll,
     data_object => $data_file,
     irods       => $irods)->metadata;

  my $updated_as_dict = {};
  for my $meta (@{$updated}) {
    $updated_as_dict->{$meta->{'attribute'}} = $meta->{'value'};
  }
  for my $key ((map { $_ . '_history'} ($STUDY_NAME, $PACBIO_STUDY_NAME))) {
    ok(exists $updated_as_dict->{$key}, "History is preserved for $key");
    delete $updated_as_dict->{$key};
  }

  my $expected_as_dict = {};
  for my $meta (@{$expected_meta}) {
     $expected_as_dict->{$meta->{'attribute'}} = $meta->{'value'};
  }
  $expected_as_dict->{$PACBIO_PLATE_NUMBER} = $plate_number;
  for my $key (($STUDY_NAME, $PACBIO_STUDY_NAME)) {
    $expected_as_dict->{$key} = $new_study_name;
  }

  is_deeply($updated_as_dict, $expected_as_dict,
    'Updated metadata is correct after the update');

  $p_row->update({qc => 1});
  $expected_as_dict->{$QC_STATE} = 1;
  for my $key ((map { $_ . '_history'} ($STUDY_NAME, $PACBIO_STUDY_NAME))) {
    delete $expected_as_dict->{$key};
  }
  # Call the updater again.
  $updater->update_secondary_metadata(\@paths_to_update); 
  # Inspect the metadata after the update.
  $updated = WTSI::NPG::HTS::DataObject->new
    (collection  => $irods_tmp_coll,
     data_object => $data_file,
     irods       => $irods)->metadata;

  $updated_as_dict = {};
  for my $meta (@{$updated}) {
    $updated_as_dict->{$meta->{'attribute'}} = $meta->{'value'};
  }
  is($expected_as_dict->{$QC_STATE}, 1, 'Updated metadata contains qc outcome');  
}

1;
