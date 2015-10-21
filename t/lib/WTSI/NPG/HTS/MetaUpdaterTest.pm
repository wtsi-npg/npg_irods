package WTSI::NPG::HTS::MetaUpdaterTest;

use strict;
use warnings;

use Log::Log4perl;
use Test::More tests => 5;

use base qw(Test::Class);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::HTS::MetaUpdater') }

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::MetaUpdater;
use WTSI::NPG::HTS::Samtools;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $fixture_counter = 0;
my $data_path = './t/data';
my $fixture_path = "$data_path/fixtures";

my $data_file = '3002_3#1';
my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;
my $samtools = `which samtools`;

my $pid = $$;

sub setup_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("MetaUpdaterTest.$pid.$fixture_counter");
  $fixture_counter++;

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

sub teardown_fixture : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

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

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    my @paths_to_update;
    foreach my $format (qw(bam cram)) {
      push @paths_to_update, "$irods_tmp_coll/$data_file.$format";
    }

    my $db_dir = File::Temp->newdir;
    my $db_file = File::Spec->catfile($db_dir, 'ml_warehouse.db');

    my $schema;
    # create_test_db produces warnings during expected use, which
    # appear mixed with test output in the terminal
    {
      local $SIG{__WARN__} = sub { };
      $schema = TestDB->new->create_test_db('WTSI::DNAP::Warehouse::Schema',
                                            './t/data/fixtures', $db_file);
    }

    my $updater = WTSI::NPG::HTS::MetaUpdater->new(irods  => $irods,
                                                   schema => $schema);

    # 1 test
    cmp_ok($updater->update_secondary_metadata(\@paths_to_update),
           '==', scalar @paths_to_update,
           'All iRODS paths processed without errors');

    foreach my $format (qw(bam cram)) {
      my $expected_meta =
        [{attribute => $ALIGNMENT,                value     => '1'},
         {attribute => $ID_RUN,                   value     => '3002'},
         {attribute => $POSITION,                 value     => '3'},
         {attribute => $LIBRARY_ID,               value     => '60186'},
         {attribute => $QC_STATE,                 value     => '0'},
         # There is currently no reference filter installed to detect
         # the test reference
         # {attribute => $REFERENCE,
         #  value     => './t/data/test_ref.fa'},
         {attribute => $SAMPLE_COMMON_NAME,
          value     => 'Streptococcus suis'},
         {attribute => $SAMPLE_CONSENT_WITHDRAWN, value     => '0'},
         {attribute => $SAMPLE_NAME,              value     => 'BM308'},
         {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000893'},
         {attribute => $STUDY_ID,                 value     => '244'},
         {attribute => $STUDY_NAME,
          value     =>
          'Discovery of sequence diversity in Streptococcus suis (Vietnam)'},
         {attribute => $STUDY_TITLE,
          value     =>
          'Discovery of sequence diversity in Streptococcus suis (Vietnam)'},
         {attribute => $TAG_INDEX,                value     => '1'}];

      my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
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
