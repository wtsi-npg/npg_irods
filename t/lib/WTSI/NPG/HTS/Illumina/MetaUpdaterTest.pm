package WTSI::NPG::HTS::Illumina::MetaUpdaterTest;

use utf8;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::DNAP::Utilities::Runnable;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::Illumina::MetaUpdater;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::Metadata;
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
my $data_path    = './t/data/illumina_meta_updater';
my $fixture_path = "./t/fixtures";

my $utf8_extra = '[UTF-8 test: Τὴ γλῶσσα μοῦ ἔδωσαν ἑλληνικὴ το σπίτι φτωχικό στις αμμουδιές του Ομήρου.]';

my $db_dir = File::Temp->newdir;
my $wh_schema;
my $lims_factory;

my $data_file      = '7915_5#1';
my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;

my $samtools_available = `which samtools_irods`;

sub setup_databases : Test(startup) {
  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  $wh_schema = TestDB->new(sqlite_utf8_enabled => 1,
                           verbose             => 0)->create_test_db
    ('WTSI::DNAP::Warehouse::Schema', "$fixture_path/ml_warehouse",
     $wh_db_file);

  $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
}

sub teardown_databases : Test(shutdown) {
  $wh_schema->storage->disconnect;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("IlluminaMetaUpdaterTest.$pid.$test_counter");
  $test_counter++;

  if ($samtools_available) {
    WTSI::DNAP::Utilities::Runnable->new
        (arguments  => ['view', '-C',
                        '-T', "$data_path/$reference_file",
                        '-o', "irods:$irods_tmp_coll/$data_file.cram",
                        "$data_path/$data_file.sam"],
         executable => 'samtools_irods')->run;
  }

  $irods->add_collection("$irods_tmp_coll/qc");
  $irods->add_object("$data_path/qc/$data_file.genotype.json",
                     "$irods_tmp_coll/qc");
  $irods->add_object("$data_path/$data_file.seqchksum", $irods_tmp_coll);
  $irods->add_object("$data_path/$data_file.composition.json", $irods_tmp_coll);
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::Illumina::MetaUpdater');
}

sub update_secondary_metadata : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $updater = WTSI::NPG::HTS::Illumina::MetaUpdater->new
    (irods       => $irods,
     lims_factory => $lims_factory);

  # Alignment files
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 3;
    }

    my @composition_files = ("$irods_tmp_coll/$data_file.composition.json");

    my $updater = WTSI::NPG::HTS::Illumina::MetaUpdater->new
      (irods       => $irods,
       lims_factory => $lims_factory);

    # 1 test
    cmp_ok($updater->update_secondary_metadata(\@composition_files),
           '==', 4, 'All files processed without errors');

    my $expected_meta =
      [{attribute => $LIBRARY_ID,               value     => '4957423'},
       {attribute => $LIBRARY_TYPE,             value     => 'No PCR'},
       {attribute => $QC_STATE,                 value     => '1'},
       {attribute => $SAMPLE_NAME,              value     => '619s040'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value     => 'ERS012323'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,                value     => '230889'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
       {attribute => $STUDY_ID,                 value     => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra}];

    my $file_name = "$data_file.cram";
    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
      (collection  => $irods_tmp_coll,
       data_object => $file_name,
       irods       => $irods);

    # 2 tests
    is_deeply($obj->metadata, $expected_meta,
              'Secondary metadata updated correctly') or
                diag explain $obj->metadata;
  } # SKIP samtools


  # Restricted
  my $qc_obj = WTSI::NPG::HTS::Illumina::AncDataObject->new
    (collection  => "$irods_tmp_coll/qc",
     data_object => "$data_file.genotype.json",
     irods       => $irods);
  is_deeply($qc_obj->metadata, [{attribute => $STUDY_ID, value => '619'}],
            "Secondary metadata updated correctly ($STUDY_ID) JSON") or
              diag explain $qc_obj->metadata;

  # Unrestricted
  my $anc_obj = WTSI::NPG::HTS::Illumina::AncDataObject->new
    (collection  => $irods_tmp_coll,
     data_object => "$data_file.seqchksum",
     irods       => $irods);
  is_deeply($anc_obj->metadata, [],
            'Secondary metadata updated correctly QC') or
              diag explain $anc_obj->metadata;
}

1;
