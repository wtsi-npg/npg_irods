package WTSI::NPG::HTS::MetaUpdaterTest;

use strict;
use warnings;

use English qw(-no_match_vars);
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw(WTSI::NPG::HTS::Test);

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::MetaUpdater;
use WTSI::NPG::HTS::Samtools;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $test_counter = 0;
my $data_path = './t/data/meta_updater';
my $fixture_path = "./t/fixtures";

my $db_dir = File::Temp->newdir;
my $wh_attr = {RaiseError    => 1,
               on_connect_do => 'PRAGMA encoding = "UTF-8"'};
my $wh_schema;
my $lims_factory;

my $data_file = '7915_5#1';
my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;
my $samtools = `which samtools`;

my $pid = $PID;

sub setup_databases : Test(startup) {
  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  my $wh_attr = {RaiseError    => 1,
                 on_connect_do => 'PRAGMA encoding = "UTF-8"'};

  {
    # create_test_db produces warnings during expected use, which
    # appear mixed with test output in the terminal
    local $SIG{__WARN__} = sub { };
    $wh_schema = TestDB->new(test_dbattr => $wh_attr)->create_test_db
      ('WTSI::DNAP::Warehouse::Schema', "$fixture_path/ml_warehouse",
       $wh_db_file);
  }

  $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
}

sub teardown_databases : Test(shutdown) {
  $wh_schema->storage->disconnect;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("MetaUpdaterTest.$pid.$test_counter");
  $test_counter++;

  if ($samtools) {
    WTSI::NPG::HTS::Samtools->new
        (arguments => ['view', '-C',
                       '-T', qq[$data_path/$reference_file],
                       '-o', qq[irods:$irods_tmp_coll/$data_file.cram]],
         path      => "$data_path/$data_file.sam")->run;

    WTSI::NPG::HTS::Samtools->new
        (arguments => ['view', '-b',
                       '-T', qq[$data_path/$reference_file],
                       '-o', qq[irods:$irods_tmp_coll/$data_file.bam]],
         path      => "$data_path/$data_file.sam")->run;
  }
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::MetaUpdater');
}

sub update_secondary_metadata : Test(3) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 3;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    my @paths_to_update;
    foreach my $format (qw(bam cram)) {
      push @paths_to_update, "$irods_tmp_coll/$data_file.$format";
    }

    my $updater = WTSI::NPG::HTS::MetaUpdater->new
      (irods       => $irods,
      lims_factory => $lims_factory);

    # 1 test
    cmp_ok($updater->update_secondary_metadata(\@paths_to_update),
           '==', scalar @paths_to_update,
           'All iRODS paths processed without errors');

    foreach my $format (qw(bam cram)) {
      my $expected_meta =
        [{attribute => $LIBRARY_ID,               value     => '4957423'},
         {attribute => $QC_STATE,                 value     => '1'},
         {attribute => $SAMPLE_NAME,              value     => '619s040'},
         {attribute => $SAMPLE_COMMON_NAME,
          value     => 'Burkholderia pseudomallei'},
         {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
         {attribute => $STUDY_NAME,
          value     => 'Burkholderia pseudomallei diversity'},
         {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
         {attribute => $STUDY_ID,                 value     => '619'},
         {attribute => $STUDY_TITLE,
          value     => 'Burkholderia pseudomallei diversity'}];

      my $obj = WTSI::NPG::HTS::AlMapFileDataObject->new
        (collection  => $irods_tmp_coll,
         data_object => "$data_file.$format",
         irods       => $irods);

      # 2 tests
      is_deeply($obj->metadata, $expected_meta,
                'Secondary metadata updated correctly') or
                  diag explain $obj->metadata;
    }
  } # SKIP samtools
}

1;
