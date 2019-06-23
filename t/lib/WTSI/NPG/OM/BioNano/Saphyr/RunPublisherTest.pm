package WTSI::NPG::OM::BioNano::Saphyr::RunPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Temp;
use File::Spec::Functions;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::OM::BioNano::Saphyr::RunPublisher;

Log::Log4perl::init('./etc/log4perl_tests.conf');


{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

{
  package MockAccessClient;

  use Moose;
  use File::Slurp;
  use File::Spec::Functions;

  with qw[
           WTSI::DNAP::Utilities::Loggable
           WTSI::NPG::OM::BioNano::Saphyr::AccessClient
        ];

  sub find_bnx_results {
    my ($self) = @_;

    my @job_results;
    foreach my $file (glob './t/data/bionano/saphyr/*.json') {
      $self->info("Reading test data from '$file'");
      my $json = read_file($file);
      push @job_results,
        WTSI::NPG::OM::BioNano::Saphyr::JobResult->new($json);
    }

    return @job_results;
  }

  sub get_bnx_file {
    my ($self, $job_id) = @_;

    my %bnx_files =
      (
          481347 => '8ae847ee-dfa4-473d-894e-ffb245f8d8c7.KHPZDTGLPQJGPNWU.1.RawMolecules.bnx.gz',
          483070 => '8ae847ee-dfa4-473d-894e-ffb245f8d8c7.KHPZDTGLPQJGPNWU.2.RawMolecules.bnx.gz'
      );

    return catfile('./t/data/bionano/saphyr', $bnx_files{$job_id});
  }
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/bionano/saphyr';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

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
    $irods->add_collection("SaphyrRunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub publish_files : Test(88) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $publisher = WTSI::NPG::OM::BioNano::Saphyr::RunPublisher->new
    (access_client   => MockAccessClient->new,
     irods           => $irods,
     dest_collection => $dest_coll,
     tmpdir          => q[.],
     mlwh_schema     => $wh_schema);

  my @run_uid_chip =
    (['8ae847ee-dfa4-473d-894e-ffb245f8d8c7', 'KHPZDTGLPQJGPNWU']);
  my @flowcell = 1 .. 2;

  my @expected_paths;
  foreach my $run_uid_chip (@run_uid_chip) {
    foreach my $flowcell (@flowcell) {
      foreach my $filename (qw[RawMolecules.bnx.gz json]) {
        push @expected_paths,
          catfile(catdir($dest_coll, @{$run_uid_chip}, $flowcell),
                  join q[.], @{$run_uid_chip}, $flowcell, $filename);
      }
    }
  }

  my ($num_files, $num_processed, $num_errors) = $publisher->publish_files;

  cmp_ok($num_files,     '==', scalar @expected_paths,
         'publish_files found the correct number of files');
  cmp_ok($num_processed, '==', scalar @expected_paths,
         'publish_files processed the correct number of files');
  cmp_ok($num_errors,    '==', 0,
         'publish_files completed without errors');

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
  check_primary_metadata($irods, @observed_paths);

  check_study_metadata($irods, @observed_paths);
  check_sample_metadata($irods, @observed_paths);


}

sub observed_data_objects {
  my ($irods, $dest_collection) = @_;

  my ($observed_paths) = $irods->list_collection($dest_collection, 'RECURSE');
  my @observed_paths = @{$observed_paths};
  @observed_paths = sort @observed_paths;

  return @observed_paths;
}

sub check_common_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($DCTERMS_CREATED, $DCTERMS_CREATOR, $DCTERMS_PUBLISHER,
                      $FILE_MD5) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_primary_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my @attrs =
      ($WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_RUN_UID,
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_SERIALNUMBER,
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_FLOWCELL,
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_SAMPLE_NAME,
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_PROJECT_NAME,
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_EXPERIMENT_NAME,
       $LIBRARY_ID);

    foreach my $attr(@attrs) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }

    deep_observed_vs_expected
        ([$obj->find_in_metadata($WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_RUN_UID)],
         [{attribute => $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_RUN_UID,
           value     => '8ae847ee-dfa4-473d-894e-ffb245f8d8c7'}],
         'Run UID has expected value');

    deep_observed_vs_expected
        ([$obj->find_in_metadata($WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_SERIALNUMBER)],
         [{attribute => $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_SERIALNUMBER,
           value     => 'KHPZDTGLPQJGPNWU'}],
         'Chip serial number has expected value');

    my ($flowcell) = $path =~
        m{/8ae847ee-dfa4-473d-894e-ffb245f8d8c7[.]KHPZDTGLPQJGPNWU[.](\d)[.]}msx;

    my ($sample_name, $saphyr_sample_name);
    if ($flowcell eq '1') {
      $sample_name = '5290STDY7575139'; # From warehouse
      $saphyr_sample_name = 'fGadMor1'; # From Saphyr database
    }
    elsif ($flowcell eq '2') {
      $sample_name = '4616STDY6965619'; # From warehouse
      $saphyr_sample_name = '25G257155090'; # From Saphyr database
    }
    else {
      fail "Unexpected Saphyr flowcell '$flowcell'";
    }

    deep_observed_vs_expected
        ([$obj->find_in_metadata($WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_FLOWCELL)],
         [{attribute => $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_FLOWCELL,
           value     => $flowcell}],
         'Chip flowcell has expected value');

    deep_observed_vs_expected
        ([$obj->find_in_metadata($SAMPLE_NAME)],
         [{attribute => $SAMPLE_NAME,
           value     => $sample_name}],
         'Saphyr sample name has expected value');

    deep_observed_vs_expected
        ([$obj->find_in_metadata($WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_SAMPLE_NAME)],
         [{attribute => $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_SAMPLE_NAME,
           value     => $saphyr_sample_name}],
         'Saphyr sample name has expected value');
  }
}

sub check_study_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($STUDY_ID) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_sample_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($SAMPLE_ID, $SAMPLE_DONOR_ID, $SAMPLE_NAME,
                      $SAMPLE_SUPPLIER_NAME) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub deep_observed_vs_expected {
  my ($observed, $expected, $message) = @_;

  is_deeply($observed, $expected, $message) or diag explain $observed;
}

1;
