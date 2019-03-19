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
      (481347 => '8ae847ee-dfa4-473d-894e-ffb245f8d8c7.KHPZDTGLPQJGPNWU.1.RawMolecules.bnx.gz',
       483070 => '8ae847ee-dfa4-473d-894e-ffb245f8d8c7.KHPZDTGLPQJGPNWU.2.RawMolecules.bnx.gz',
       133195 => 'cbf987c1-f073-4ba9-ade0-6e9a89416d97.ABRTCLOLPTMWPNWU.1.RawMolecules.bnx.gz',
       133196 => 'cbf987c1-f073-4ba9-ade0-6e9a89416d97.ABRTCLOLPTMWPNWU.2.RawMolecules.bnx.gz',
       585740 => 'e568ab15-f745-4d15-b603-88d22dccd8c3.ITDCQTGLPQLGPNWU.1.RawMolecules.bnx.gz',
       585741 => 'e568ab15-f745-4d15-b603-88d22dccd8c3.ITDCQTGLPQLGPNWU.2.RawMolecules.bnx.gz');

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

sub publish_files : Test(136) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $publisher = WTSI::NPG::OM::BioNano::Saphyr::RunPublisher->new
    (access_client   => MockAccessClient->new,
     irods           => $irods,
     dest_collection => $dest_coll,
     tmpdir          => q[.]);

  my @run_uid_chip =
    (['8ae847ee-dfa4-473d-894e-ffb245f8d8c7', 'KHPZDTGLPQJGPNWU'],
     ['cbf987c1-f073-4ba9-ade0-6e9a89416d97', 'ABRTCLOLPTMWPNWU'],
     ['e568ab15-f745-4d15-b603-88d22dccd8c3', 'ITDCQTGLPQLGPNWU']);
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

 TODO: {
    todo_skip 'ML warehouse support for Saphyr not yet available', 12;

    qcheck_study_metadata($irods, @observed_paths);
  }
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
       $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_EXPERIMENT_NAME);

    foreach my $attr(@attrs) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
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

1;
