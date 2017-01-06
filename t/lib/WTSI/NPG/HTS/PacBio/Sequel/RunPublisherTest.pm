package WTSI::NPG::HTS::PacBio::Sequel::RunPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';  
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/sequel';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $wh_schema;

my $irods_tmp_coll;

sub setup_databases : Test(startup) {
  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  # my $wh_db_file = 'ml_wh.db';
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
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::RunPublisher');
}

sub list_xml_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A01", $_) }
    ('m54097_161207_133626.metadata.xml');

  is_deeply($pub->list_xml_files('1_A01','metadata',1), \@expected_paths,
     'Found meta XML file 1_A01');
}

sub list_sequence_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A01", $_) }
    ('m54097_161207_133626.scraps.bam',
     'm54097_161207_133626.subreads.bam'
    );

  is_deeply($pub->list_sequence_files('1_A01'), \@expected_paths,
     'Found sequence files A01_1');
}

sub list_index_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A01", $_) }
    ('m54097_161207_133626.scraps.bam.pbi',
     'm54097_161207_133626.subreads.bam.pbi'
    );

  is_deeply($pub->list_index_files('1_A01'), \@expected_paths,
     'Found sequence index files 1_A01');
}


sub publish_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  my $num_expected = 7;

  cmp_ok($num_processed, '==', $num_expected, "Published $num_expected files");
}

sub publish_xml_files : Test(19) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = "$irods_tmp_coll/publish_xml_files";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m54097_161207_133626.metadata.xml',
     'm54097_161207_133626.sts.xml',
     'm54097_161207_133626.subreadset.xml');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_xml_files('1_A01', 'metadata|subreadset|sts',3);
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named metadata XML files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

}

sub publish_sequence_files : Test(32) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = "$irods_tmp_coll/publish_sequence_files";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m54097_161207_133626.scraps.bam',
     'm54097_161207_133626.subreads.bam');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sequence_files('1_A01');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_study_metadata($irods, @observed_paths);
}

sub publish_index_files : Test(14) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20161207_132758";
  my $dest_coll = "$irods_tmp_coll/publish_sequence_files";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m54097_161207_133626.scraps.bam.pbi',
     'm54097_161207_133626.subreads.bam.pbi');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_index_files('1_A01');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named index files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
}


sub observed_data_objects {
  my ($irods, $dest_collection, $regex) = @_;

  my ($observed_paths) = $irods->list_collection($dest_collection, 'RECURSE');
  my @observed_paths = @{$observed_paths};
  if ($regex) {
    @observed_paths = grep { m{$regex}msx } @observed_paths;
  }
  @observed_paths = sort @observed_paths;

  return @observed_paths;
}

sub check_common_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($DCTERMS_CREATED, $DCTERMS_CREATOR, $DCTERMS_PUBLISHER,
                      $FILE_TYPE, $FILE_MD5) {
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

    foreach my $attr
      ($WTSI::NPG::HTS::PacBio::Annotator::PACBIO_CELL_INDEX,
       $WTSI::NPG::HTS::PacBio::Annotator::PACBIO_COLLECTION_NUMBER,
       $WTSI::NPG::HTS::PacBio::Annotator::PACBIO_INSTRUMENT_NAME,
       $WTSI::NPG::HTS::PacBio::Annotator::PACBIO_RUN,
       $WTSI::NPG::HTS::PacBio::Annotator::PACBIO_WELL,
       $WTSI::NPG::HTS::PacBio::Annotator::PACBIO_SAMPLE_LOAD_NAME) {
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

    # study_name is legacy metadata
    foreach my $attr ($STUDY_ID, $STUDY_NAME, $STUDY_ACCESSION_NUMBER) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}


1;
