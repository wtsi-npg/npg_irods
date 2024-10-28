package WTSI::NPG::HTS::PacBio::IsoSeqPublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Copy::Recursive qw(dircopy);
use File::Spec::Functions;
use File::Temp;
use File::Which;
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];
use WTSI::NPG::HTS::LocationWriterTest;

use WTSI::NPG::HTS::PacBio::IsoSeqPublisher;

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
my $data_path    = 't/data/pacbio/analysis';
my $rundata_path = 't/data/pacbio/sequence';
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
    $irods->add_collection("PacBioIsoSeqPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::IsoSeqPublisher');
}

sub publish_files_1 : Test(5) {
## Read Segmentation and Iso-Seq with Reference only & 5 IsoSeq barcodes

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000020083";
  my $runfolder_path = "$analysis_path/outputs",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::IsoSeqPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path,
     analysis_id     => '20083',);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A01/20083", $_) }
    ('20083.m84047_240607_151621_s1.collapse_isoforms-1.fasta.gz',
     '20083.m84047_240607_151621_s1.collapse_isoforms-1.gff',
     '20083.m84047_240607_151621_s1.collapse_isoforms-2.fasta.gz',
     '20083.m84047_240607_151621_s1.collapse_isoforms-2.gff',
     '20083.m84047_240607_151621_s1.collapse_isoforms-3.fasta.gz',
     '20083.m84047_240607_151621_s1.collapse_isoforms-3.gff',
     '20083.m84047_240607_151621_s1.collapse_isoforms-4.fasta.gz',
     '20083.m84047_240607_151621_s1.collapse_isoforms-4.gff',
     '20083.m84047_240607_151621_s1.collapse_isoforms-5.fasta.gz',
     '20083.m84047_240607_151621_s1.collapse_isoforms-5.gff',
     '20083.m84047_240607_151621_s1.collapse_isoforms.flnc_count-1.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.flnc_count-2.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.flnc_count-3.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.flnc_count-4.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.flnc_count-5.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.group-1.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.group-2.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.group-3.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.group-4.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.group-5.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.read_stat-1.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.read_stat-2.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.read_stat-3.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.read_stat-4.txt',
     '20083.m84047_240607_151621_s1.collapse_isoforms.read_stat-5.txt',
     '20083.m84047_240607_151621_s1.flnc-1.bam',
     '20083.m84047_240607_151621_s1.flnc-2.bam',
     '20083.m84047_240607_151621_s1.flnc-3.bam',
     '20083.m84047_240607_151621_s1.flnc-4.bam',
     '20083.m84047_240607_151621_s1.flnc-5.bam',
     '20083.m84047_240607_151621_s1.flnc.report-1.csv',
     '20083.m84047_240607_151621_s1.flnc.report-2.csv',
     '20083.m84047_240607_151621_s1.flnc.report-3.csv',
     '20083.m84047_240607_151621_s1.flnc.report-4.csv',
     '20083.m84047_240607_151621_s1.flnc.report-5.csv',
     '20083.m84047_240607_151621_s1.isoseq.report.json',
     '20083.m84047_240607_151621_s1.isoseq_mapping.report.json',
     '20083.m84047_240607_151621_s1.isoseq_primers.report.json',
     '20083.m84047_240607_151621_s1.mapped-1.bam',
     '20083.m84047_240607_151621_s1.mapped-2.bam',
     '20083.m84047_240607_151621_s1.mapped-3.bam',
     '20083.m84047_240607_151621_s1.mapped-4.bam',
     '20083.m84047_240607_151621_s1.mapped-5.bam',
     '20083.m84047_240607_151621_s1.mapped.bam-1.bai',
     '20083.m84047_240607_151621_s1.mapped.bam-2.bai',
     '20083.m84047_240607_151621_s1.mapped.bam-3.bai',
     '20083.m84047_240607_151621_s1.mapped.bam-4.bai',
     '20083.m84047_240607_151621_s1.mapped.bam-5.bai',
     '20083.m84047_240607_151621_s1.read_segmentation.report.json',
     '20083.m84047_240607_151621_s1.sample0.transcripts.cluster_report.csv',
     '20083.m84047_240607_151621_s1.sample0.transcripts.fl_counts.csv',
     '20083.m84047_240607_151621_s1.sample1.transcripts.cluster_report.csv',
     '20083.m84047_240607_151621_s1.sample1.transcripts.fl_counts.csv',
     '20083.m84047_240607_151621_s1.sample2.transcripts.cluster_report.csv',
     '20083.m84047_240607_151621_s1.sample2.transcripts.fl_counts.csv',
     '20083.m84047_240607_151621_s1.sample3.transcripts.cluster_report.csv',
     '20083.m84047_240607_151621_s1.sample3.transcripts.fl_counts.csv',
     '20083.m84047_240607_151621_s1.sample4.transcripts.cluster_report.csv',
     '20083.m84047_240607_151621_s1.sample4.transcripts.fl_counts.csv',
     '20083.m84047_240607_151621_s1.transcripts-1.fasta.gz',
     '20083.m84047_240607_151621_s1.transcripts-2.fasta.gz',
     '20083.m84047_240607_151621_s1.transcripts-3.fasta.gz',
     '20083.m84047_240607_151621_s1.transcripts-4.fasta.gz',
     '20083.m84047_240607_151621_s1.transcripts-5.fasta.gz',
     );

  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  my $load_file = "$runfolder_path/20083.m84047_240607_151621_s1.loaded.txt";
  ok(-e $load_file, "Successful loading file created");
  unlink $load_file;
}


sub publish_files_2 : Test(5) {
## Read Segmentation and Iso-Seq with no reference & 5 IsoSeq barcodes

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000020156";
  my $runfolder_path = "$analysis_path/outputs",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::IsoSeqPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path,
     analysis_id     => '20156',);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A01/20156", $_) }
    ('20156.m84047_240607_151621_s1.collapse_isoforms.fasta.gz',
     '20156.m84047_240607_151621_s1.collapse_isoforms.fl_count.txt',
     '20156.m84047_240607_151621_s1.collapse_isoforms.flnc_count.txt',
     '20156.m84047_240607_151621_s1.collapse_isoforms.gff',
     '20156.m84047_240607_151621_s1.collapse_isoforms.group.txt',
     '20156.m84047_240607_151621_s1.collapse_isoforms.read_stat.txt',
     '20156.m84047_240607_151621_s1.flnc-1.bam',
     '20156.m84047_240607_151621_s1.flnc-2.bam',
     '20156.m84047_240607_151621_s1.flnc-3.bam',
     '20156.m84047_240607_151621_s1.flnc-4.bam',
     '20156.m84047_240607_151621_s1.flnc-5.bam',
     '20156.m84047_240607_151621_s1.flnc.report-1.csv',
     '20156.m84047_240607_151621_s1.flnc.report-2.csv',
     '20156.m84047_240607_151621_s1.flnc.report-3.csv',
     '20156.m84047_240607_151621_s1.flnc.report-4.csv',
     '20156.m84047_240607_151621_s1.flnc.report-5.csv',
     '20156.m84047_240607_151621_s1.isoseq.report.json',
     '20156.m84047_240607_151621_s1.isoseq_mapping.report.json',
     '20156.m84047_240607_151621_s1.isoseq_primers.report.json',
     '20156.m84047_240607_151621_s1.mapped.bam',
     '20156.m84047_240607_151621_s1.mapped.bam.bai',
     '20156.m84047_240607_151621_s1.read_segmentation.report.json',
     '20156.m84047_240607_151621_s1.sample0.transcripts.cluster_report.csv',
     '20156.m84047_240607_151621_s1.sample0.transcripts.fl_counts.csv',
     '20156.m84047_240607_151621_s1.transcripts.fasta.gz',
     );

  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  my $load_file = "$runfolder_path/20156.m84047_240607_151621_s1.loaded.txt";
  ok(-e $load_file, "Successful loading file created");
  unlink $load_file;
}

sub publish_files_3 : Test(5) {
## Read Segmentation and Iso-Seq with Reference only & 5 IsoSeq barcodes.
## csi not bai mapped bam indexes due to large reference size

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $analysis_path  = "$data_path/0000021786";
  my $runfolder_path = "$analysis_path/outputs",
  my $dest_coll      = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::IsoSeqPublisher->new
    (restart_file    => catfile($tmpdir->dirname, 'published.json'),
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     analysis_path   => $analysis_path,
     runfolder_path  => $runfolder_path,
     analysis_id     => '21786',);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A01/21786", $_) }
    ('21786.m84093_241017_151706_s1.collapse_isoforms-1.fasta.gz',
     '21786.m84093_241017_151706_s1.collapse_isoforms-1.gff',
     '21786.m84093_241017_151706_s1.collapse_isoforms-2.fasta.gz',
     '21786.m84093_241017_151706_s1.collapse_isoforms-2.gff',
     '21786.m84093_241017_151706_s1.collapse_isoforms-3.fasta.gz',
     '21786.m84093_241017_151706_s1.collapse_isoforms-3.gff',
     '21786.m84093_241017_151706_s1.collapse_isoforms-4.fasta.gz',
     '21786.m84093_241017_151706_s1.collapse_isoforms-4.gff',
     '21786.m84093_241017_151706_s1.collapse_isoforms-5.fasta.gz',
     '21786.m84093_241017_151706_s1.collapse_isoforms-5.gff',
     '21786.m84093_241017_151706_s1.collapse_isoforms.flnc_count-1.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.flnc_count-2.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.flnc_count-3.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.flnc_count-4.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.flnc_count-5.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.group-1.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.group-2.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.group-3.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.group-4.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.group-5.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.read_stat-1.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.read_stat-2.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.read_stat-3.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.read_stat-4.txt',
     '21786.m84093_241017_151706_s1.collapse_isoforms.read_stat-5.txt',
     '21786.m84093_241017_151706_s1.flnc-1.bam',
     '21786.m84093_241017_151706_s1.flnc-2.bam',
     '21786.m84093_241017_151706_s1.flnc-3.bam',
     '21786.m84093_241017_151706_s1.flnc-4.bam',
     '21786.m84093_241017_151706_s1.flnc-5.bam',
     '21786.m84093_241017_151706_s1.flnc.report-1.csv',
     '21786.m84093_241017_151706_s1.flnc.report-2.csv',
     '21786.m84093_241017_151706_s1.flnc.report-3.csv',
     '21786.m84093_241017_151706_s1.flnc.report-4.csv',
     '21786.m84093_241017_151706_s1.flnc.report-5.csv',
     '21786.m84093_241017_151706_s1.isoseq.report.json',
     '21786.m84093_241017_151706_s1.isoseq_mapping.report.json',
     '21786.m84093_241017_151706_s1.isoseq_primers.report.json',
     '21786.m84093_241017_151706_s1.mapped-1.bam',
     '21786.m84093_241017_151706_s1.mapped-1.bam.csi',
     '21786.m84093_241017_151706_s1.mapped-2.bam',
     '21786.m84093_241017_151706_s1.mapped-2.bam.csi',
     '21786.m84093_241017_151706_s1.mapped-3.bam',
     '21786.m84093_241017_151706_s1.mapped-3.bam.csi',
     '21786.m84093_241017_151706_s1.mapped-4.bam',
     '21786.m84093_241017_151706_s1.mapped-4.bam.csi',
     '21786.m84093_241017_151706_s1.mapped-5.bam',
     '21786.m84093_241017_151706_s1.mapped-5.bam.csi',
     '21786.m84093_241017_151706_s1.read_segmentation.report.json',
     '21786.m84093_241017_151706_s1.sample0.transcripts.cluster_report.csv',
     '21786.m84093_241017_151706_s1.sample0.transcripts.fl_counts.csv',
     '21786.m84093_241017_151706_s1.sample1.transcripts.cluster_report.csv',
     '21786.m84093_241017_151706_s1.sample1.transcripts.fl_counts.csv',
     '21786.m84093_241017_151706_s1.sample2.transcripts.cluster_report.csv',
     '21786.m84093_241017_151706_s1.sample2.transcripts.fl_counts.csv',
     '21786.m84093_241017_151706_s1.sample3.transcripts.cluster_report.csv',
     '21786.m84093_241017_151706_s1.sample3.transcripts.fl_counts.csv',
     '21786.m84093_241017_151706_s1.sample4.transcripts.cluster_report.csv',
     '21786.m84093_241017_151706_s1.sample4.transcripts.fl_counts.csv',
     '21786.m84093_241017_151706_s1.transcripts-1.fasta.gz',
     '21786.m84093_241017_151706_s1.transcripts-2.fasta.gz',
     '21786.m84093_241017_151706_s1.transcripts-3.fasta.gz',
     '21786.m84093_241017_151706_s1.transcripts-4.fasta.gz',
     '21786.m84093_241017_151706_s1.transcripts-5.fasta.gz',
     );

  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  my $load_file = "$runfolder_path/21786.m84093_241017_151706_s1.loaded.txt";
  ok(-e $load_file, "Successful loading file created");
  unlink $load_file;

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

1;
