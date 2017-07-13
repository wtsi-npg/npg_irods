package WTSI::NPG::HTS::PacBio::RunPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::RunPublisher;
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
my $data_path    = 't/data/pacbio';
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

sub list_meta_xml_file : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/24862_627";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  is($pub->list_meta_xml_file('A01_1'),
     catfile("$runfolder_path/A01_1",
             'm131209_183112_00127_c100579142550000001823092301191430_s1_p0.metadata.xml'),
     'Found meta XML file A01_1');
}

sub list_basx_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/24862_627";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/A01_1/Analysis_Results", $_) }
    ('m131209_183112_00127_c100579142550000001823092301191430_s1_p0.1.bax.h5',
     'm131209_183112_00127_c100579142550000001823092301191430_s1_p0.2.bax.h5',
     'm131209_183112_00127_c100579142550000001823092301191430_s1_p0.3.bax.h5',
     'm131209_183112_00127_c100579142550000001823092301191430_s1_p0.bas.h5');

  is_deeply($pub->list_basx_files('A01_1'), \@expected_paths,
            'Found bas/x files A01_1');
}

sub list_sts_xml_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/24862_627";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/A01_1/Analysis_Results", $_) }
    ('m131209_183112_00127_c100579142550000001823092301191430_s1_p0.sts.xml');

  is_deeply($pub->list_sts_xml_files('A01_1'), \@expected_paths,
            'Found sts XML files A01_1');
}

sub publish_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/45137_1095";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  my $num_expected = 42;

  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', $num_expected, "Published $num_expected files");
}

sub publish_meta_xml_files : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/45137_1095";
  my $dest_coll = "$irods_tmp_coll/publish_meta_xml_file";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/A01_1", $_) }
    ('m160322_123501_00127_c100951322550000001823215306251640_s1_p0.metadata.xml');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_meta_xml_file('A01_1');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named metadata XML files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
}

sub publish_basx_files : Test(68) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/45137_1095";
  my $dest_coll = "$irods_tmp_coll/publish_basx_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/A01_1/Analysis_Results", $_) }
    ('m160322_123501_00127_c100951322550000001823215306251640_s1_p0.1.bax.h5',
     'm160322_123501_00127_c100951322550000001823215306251640_s1_p0.2.bax.h5',
     'm160322_123501_00127_c100951322550000001823215306251640_s1_p0.3.bax.h5',
     'm160322_123501_00127_c100951322550000001823215306251640_s1_p0.bas.h5');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_basx_files('A01_1');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named basx files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_study_metadata($irods, @observed_paths);
}

sub publish_sts_xml_files : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/45137_1095";
  my $dest_coll = "$irods_tmp_coll/publish_sts_xml_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/A01_1/Analysis_Results", $_) }
    ('m160322_123501_00127_c100951322550000001823215306251640_s1_p0.sts.xml');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sts_xml_files('A01_1');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sts XML files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
}

sub publish_multiplexed : Test(80) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/superfoo/39859_968";
  my $dest_coll = "$irods_tmp_coll/publish_multiplexed";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/E01_1/Analysis_Results", $_) }
    ('m150718_080850_00127_c100822662550000001823177111031595_s1_p0.1.bax.h5',
     'm150718_080850_00127_c100822662550000001823177111031595_s1_p0.2.bax.h5',
     'm150718_080850_00127_c100822662550000001823177111031595_s1_p0.3.bax.h5',
     'm150718_080850_00127_c100822662550000001823177111031595_s1_p0.bas.h5');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_basx_files('E01_1');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named basx files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_study_metadata($irods, @observed_paths);
  check_multiplex_metadata($irods, 2, @observed_paths);
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
                      $FILE_MD5) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }

    # Two copies on h5 files because of legacy metadata 'type' => 'bas'
    my $num_types = ($path =~ m{\.h5$}) ? 2 : 1;
    my @avu = $obj->find_in_metadata($FILE_TYPE);
    cmp_ok(scalar @avu, '==', $num_types,
           "$file_name $FILE_TYPE x$num_types metadata present");
  }
}

sub check_primary_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr
      ($PACBIO_CELL_INDEX,
       $PACBIO_COLLECTION_NUMBER,
       $PACBIO_INSTRUMENT_NAME,
       $PACBIO_RUN,
       $PACBIO_SET_NUMBER,
       $PACBIO_WELL,
       $PACBIO_SAMPLE_LOAD_NAME) {
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
    foreach my $attr ($STUDY_ID, $STUDY_NAME, $STUDY_ACCESSION_NUMBER,
                      $PACBIO_STUDY_NAME) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_multiplex_metadata {
  my ($irods, $n, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    ok($obj->find_in_metadata
       ($WTSI::NPG::HTS::PacBio::Annotator::PACBIO_MULTIPLEX),
       "$file_name multiplex metadata present");

    # We can't guarantee that there will be n x other sample metadata
    foreach my $attr ($TAG_SEQUENCE, $SAMPLE_ID) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', $n, "$file_name $n x $attr metadata present");
    }
  }
}

1;
