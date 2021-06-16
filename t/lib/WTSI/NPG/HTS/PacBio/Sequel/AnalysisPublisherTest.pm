package WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher;
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
my $data_path    = 't/data/pacbio/sequel_analysis';
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
    $irods->add_collection("PacBioSequelAnalysisPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher');
}

sub list_files : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001612";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths1 =
    map { catfile($runfolder_path, $_) }
    ('lima_output.lbc12--lbc12.bam',
     'lima_output.lbc5--lbc5.bam',
     'lima_output.removed.bam');

  is_deeply($pub->list_files('bam$'), \@expected_paths1,
     'Found sequence files for 001612');

  my @expected_paths2 =
    map { catfile($runfolder_path, $_) }
    ('lima_output.lbc12--lbc12.bam.pbi',
     'lima_output.lbc5--lbc5.bam.pbi',
     'lima_output.removed.bam.pbi');

  is_deeply($pub->list_files('pbi$'), \@expected_paths2,
     'Found sequence index files for 001612');

  my @expected_paths3 =
    map { catfile($runfolder_path, $_) }
    ('lima_output.lbc12--lbc12.subreadset.xml',
     'lima_output.lbc5--lbc5.subreadset.xml',
     'lima_output.removed.subreadset.xml');

  is_deeply($pub->list_files('subreadset.xml$'), \@expected_paths3,
     'Found sequence index files for 001612');
}

sub publish_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001612";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = "$irods_tmp_coll/publish_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  my $num_expected = 10;

  cmp_ok($num_processed, '==', $num_expected, "Published $num_expected files");
  cmp_ok($num_errors,    '==', 0);
}

sub publish_xml_files : Test(19) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001612";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = "$irods_tmp_coll/publish_xml_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/2_B01", $_) }
    ('lima_output.lbc12--lbc12.subreadset.xml',
     'lima_output.lbc5--lbc5.subreadset.xml',
      'lima_output.removed.subreadset.xml');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_non_sequence_files('subreadset.xml$');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named metadata XML files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
}

sub publish_sequence_files : Test(61) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001612";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/2_B01", $_) }
    ('lima_output.lbc12--lbc12.bam',
     'lima_output.lbc5--lbc5.bam',
     'lima_output.removed.bam');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sequence_files;
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, 0, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_secondary_metadata($irods, @observed_paths);

  unlink $pub->restart_file;
}

sub publish_index_files : Test(19) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001612";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = "$irods_tmp_coll/publish_index_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/2_B01", $_) }
    ('lima_output.lbc12--lbc12.bam.pbi',
     'lima_output.lbc5--lbc5.bam.pbi',
     'lima_output.removed.bam.pbi',);

  my ($num_files, $num_processed, $num_errors) =
     $pub->publish_non_sequence_files('pbi$');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named index files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

  unlink $pub->restart_file;
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
  my ($irods, $skip_target_check, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);
    my @avu_plex = $obj->find_in_metadata($PACBIO_MULTIPLEX);

    foreach my $attr
      ($PACBIO_CELL_INDEX,
       $PACBIO_COLLECTION_NUMBER,
       $PACBIO_DATA_LEVEL,
       $PACBIO_INSTRUMENT_NAME,
       $PACBIO_RUN,
       $PACBIO_WELL,
       $PACBIO_SAMPLE_LOAD_NAME
      ) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
    if (! $skip_target_check) {
        foreach my $attr ($TARGET) {
            my @avu = $obj->find_in_metadata($attr);
            my $expected = scalar (@avu_plex == 1) ? 0 : 1;
            cmp_ok(scalar @avu,'==', $expected, "$file_name $attr metadata correct");
        }
    }
  }
}

sub check_secondary_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my @avu_plex = $obj->find_in_metadata($PACBIO_MULTIPLEX);

    # study_name is legacy metadata
    foreach my $attr ($STUDY_ID, $STUDY_NAME, $STUDY_ACCESSION_NUMBER,
                      $PACBIO_STUDY_NAME) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }

    foreach my $attr ($TAG_INDEX, $TAG_SEQUENCE) {
      my @avu = $obj->find_in_metadata($attr);
      my $operator = scalar (@avu_plex == 1) ? '>' : '==';
      cmp_ok(scalar @avu, $operator, 1, "$file_name $attr metadata present");
    }

  }
}

sub list_files_2 : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/000226";
  my $runfolder_path = "$analysis_path/tasks/pbcoretools.tasks.auto_ccs_outputs-0",
  my $dest_coll      = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths1 =
    map { catfile($runfolder_path, $_) }
    ('m64016_190608_025655.ccs.bam');

  is_deeply($pub->list_files('bam$'), \@expected_paths1,
     'Found sequence files for 000226');

  my @expected_paths2 =
    map { catfile($runfolder_path, $_) }
    ('m64016_190608_025655.ccs.bam.pbi');

  is_deeply($pub->list_files('pbi$'), \@expected_paths2,
     'Found sequence index files for 001612');

}

sub publish_files_2 : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/000226";
  my $runfolder_path = "$analysis_path/tasks/pbcoretools.tasks.auto_ccs_outputs-0",
  my $dest_coll      = "$irods_tmp_coll/publish_files_2";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  my $num_expected = 3;

  cmp_ok($num_processed, '==', $num_expected, "Published $num_expected files");
  cmp_ok($num_errors,    '==', 0);
}


sub publish_sequence_files_2 : Test(40) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/001185";
  my $runfolder_path = "$analysis_path/tasks/barcoding.tasks.lima-0",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/2_B01", $_) }
    ('lima.bc1022_BAK8B_OA--bc1022_BAK8B_OA.bam',
     'lima.removed.bam');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sequence_files;
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, 1, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_secondary_metadata($irods, @observed_paths);

  unlink $pub->restart_file;
}


sub publish_sequence_files_3 : Test(4) {
## run 81230 cell B01 - expected deplexing

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000003442";
  my $runfolder_path = "$analysis_path/cromwell-job/call-demultiplex_barcodes/call-lima/execution",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/2_B01", $_) }
    ('demultiplex.bc1017_BAK8B_OA--bc1017_BAK8B_OA.bam',
     'demultiplex.removed.bam');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sequence_files;
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  unlink $pub->restart_file;
}


sub publish_sequence_files_4 : Test(1) {
## run 81230 cell B01 - unexpected barcode

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000003280";
  my $runfolder_path = "$analysis_path/cromwell-job/call-demultiplex_barcodes/call-lima/execution",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  throws_ok { $pub->publish_sequence_files } qr /Unexpected barcode/, 
    'Correctly failed to publish data from unexpected barcode';

  unlink $pub->restart_file;
}

sub publish_sequence_files_5 : Test(4) {
## run 81876 cell A01 - ccs only analysis creating hifi_reads.bam

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000003499";
  my $runfolder_path = "$analysis_path/cromwell-job/call-export_bam/execution",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m64089e_210503_164858.hifi_reads.bam');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_sequence_files;
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  unlink $pub->restart_file;
}

sub publish_files_3 : Test(1) {
## run 81230 cell B01 - qc check failed

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000003280";
  my $runfolder_path = "$analysis_path/cromwell-job/call-demultiplex_barcodes/call-lima/execution",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path);

  throws_ok { $pub->publish_files } qr /QC check failed/, 
    'Correctly failed to publish qc fail data';

  unlink $pub->restart_file;
}

1;
