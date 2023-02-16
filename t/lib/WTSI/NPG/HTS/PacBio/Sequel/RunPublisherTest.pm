package WTSI::NPG::HTS::PacBio::Sequel::RunPublisherTest;

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

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;
use WTSI::NPG::HTS::PacBio::Sequel::Product;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

{
  package TestAPIClient;
  use Moose;

  extends 'WTSI::NPG::HTS::PacBio::Sequel::APIClient';
  override 'query_dataset_reports' => sub { my @r; return [@r]; }
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/sequel';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $SEQUENCE_PRODUCT      = $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::SEQUENCE_PRODUCT;
my $SEQUENCE_AUXILIARY    = $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::SEQUENCE_AUXILIARY;
my $FILE_PREFIX_PATTERN   = $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::FILE_PREFIX_PATTERN;
my $SEQUENCE_FILE_FORMAT  = $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::SEQUENCE_FILE_FORMAT;
my $SEQUENCE_INDEX_FORMAT = $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::SEQUENCE_INDEX_FORMAT;

my $wh_schema;

my $irods_tmp_coll;

if (!which "generate_pac_bio_id"){
  plan skip_all => "Pac Bio product_id generation script not installed"
}

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
    $irods->add_collection("PacBioSequelRunPublisherTest.$pid.$test_counter");
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
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.subreadset.xml');

  is_deeply($pub->list_files('1_A02',
     $FILE_PREFIX_PATTERN .'[.]'. '(' . 'subreadset' .')[.]xml$',1),
     \@expected_paths, 'Found meta XML file 1_A02');
}

sub list_aux_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.adapters.fasta');

  is_deeply($pub->list_files('1_A02', 
     $FILE_PREFIX_PATTERN .'[.]adapters[.]fasta$'), \@expected_paths,
     'Found adapter fasta file 1_A02');
}

sub list_sequence_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths1 =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.subreads.bam');

  my @expected_paths2 =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.scraps.bam');

  is_deeply($pub->list_files('1_A02', 
    $FILE_PREFIX_PATTERN .q{[.]}.$SEQUENCE_PRODUCT.q{[.]}.$SEQUENCE_FILE_FORMAT .q{$}), 
    \@expected_paths1, 'Found sequence files 1: A01_1');
  is_deeply($pub->list_files('1_A02',
    $FILE_PREFIX_PATTERN .q{[.]}.$SEQUENCE_AUXILIARY.q{[.]}.$SEQUENCE_FILE_FORMAT .q{$}), 
    \@expected_paths2, 'Found sequence files 2: A01_1');
}

sub list_index_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.scraps.bam.pbi',
     'm54097_170727_170646.subreads.bam.pbi');

  my $seq_types = qq{($SEQUENCE_PRODUCT|$SEQUENCE_AUXILIARY)};
  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $seq_types . q{[.]}.
        $SEQUENCE_FILE_FORMAT .q{[.]}. $SEQUENCE_INDEX_FORMAT .q{$};

  is_deeply($pub->list_files('1_A02',$file_pattern), \@expected_paths,
            'Found sequence index files 1_A02');
}

sub list_image_archive_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = $irods_tmp_coll;

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$runfolder_path/1_A02", $_) }
    ('m54097_170727_170646.primary_qc.tar.xz');

  is_deeply($pub->list_files('1_A02',$FILE_PREFIX_PATTERN .'[.]primary_qc[.]tar[.]xz$'),
    \@expected_paths, 'Found image archive files 1_A02');
}

sub publish_files_on_instrument_1 : Test(42) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r64174e_20210114_161659";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r64174e_20210114_161659');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A01") or die "Chmod 0770 directory failed : $!";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $tmprf_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m64174e_210114_162751.consensusreadset.xml',
     'm64174e_210114_162751.primary_qc.tar.xz',
     'm64174e_210114_162751.reads.bam',
     'm64174e_210114_162751.reads.bam.pbi',
     'm64174e_210114_162751.sts.xml',
     'm64174e_210114_162751.zmw_metrics.json.gz');

  cmp_ok($num_processed, '==', scalar @expected_paths,
     "Published on instrument files correctly");
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named on instrument files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
  my @seq_paths = grep /.bam$/, @observed_paths;
  check_primary_metadata($irods, $pub, @seq_paths);
 
  unlink $pub->restart_file;
}

sub publish_files_on_instrument_2 : Test(86) {
## on instrument deplexing: 1 cell

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r64089e_20220615_171559";
  my $dest_coll = "$irods_tmp_coll/publish_files";
  my $expected_json = 't/data/mlwh_json/pacbio.json';

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();
  
  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r64089e_20220615_171559');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A01") or die "Chmod 0770 directory failed : $!";
  
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $tmprf_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m64089e_220615_173331.bc1015_BAK8B_OA--bc1015_BAK8B_OA.consensusreadset.xml',
     'm64089e_220615_173331.consensusreadset.xml',
     'm64089e_220615_173331.hifi_reads.bc1015_BAK8B_OA--bc1015_BAK8B_OA.bam',
     'm64089e_220615_173331.hifi_reads.bc1015_BAK8B_OA--bc1015_BAK8B_OA.bam.pbi',
     'm64089e_220615_173331.primary_qc.tar.xz',
     'm64089e_220615_173331.sts.xml',
     'm64089e_220615_173331.unbarcoded.consensusreadset.xml',
     'm64089e_220615_173331.unbarcoded.hifi_reads.bam',
     'm64089e_220615_173331.unbarcoded.hifi_reads.bam.pbi',
     'm64089e_220615_173331.zmw_metrics.json.gz',
     'merged_analysis_report.json',);
  
  my @observed_paths = observed_data_objects($irods, $dest_coll);
 
  cmp_ok($num_processed, '==', scalar @expected_paths,
      "Published on instrument files correctly");
  cmp_ok($num_errors,    '==', 0);
  
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named on instrument files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);
  my @seq_paths = grep /.bam$/, @observed_paths;
  check_primary_metadata($irods, $pub, @seq_paths);
  check_study_metadata($irods, @seq_paths);

  my $mlwh_json = $pub->mlwh_locations->path;
  ok(-e $mlwh_json, "mlwh loader json file $mlwh_json was written by publisher");
  is_deeply(read_json_content($mlwh_json),
    set_destination(read_json_content($expected_json), $irods_tmp_coll),
    "contents of $mlwh_json are correct");

}

sub publish_files_on_instrument_3 : Test(3) {
## on instrument deplexing: 2 cells - 1 cell (A1) failed to deplex

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r64089e_20220930_164018";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r64089e_20220930_164018');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A01") or die "Chmod 0770 directory failed : $!";  
  chmod (0770, "$tmprf_path/2_B01") or die "Chmod 0770 directory failed : $!";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $tmprf_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_pathsA =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m64089e_220930_165238.consensusreadset.xml',
     'm64089e_220930_165238.primary_qc.tar.xz',
     'm64089e_220930_165238.sts.xml',
     'm64089e_220930_165238.unbarcoded.consensusreadset.xml',
     'm64089e_220930_165238.unbarcoded.hifi_reads.bam',
     'm64089e_220930_165238.unbarcoded.hifi_reads.bam.pbi',
     'm64089e_220930_165238.zmw_metrics.json.gz',);

  my @expected_pathsB =
    map { catfile("$dest_coll/2_B01", $_) }
    ('m64089e_221001_201630.bc1012_BAK8A_OA--bc1012_BAK8A_OA.consensusreadset.xml',
     'm64089e_221001_201630.consensusreadset.xml',
     'm64089e_221001_201630.hifi_reads.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam',
     'm64089e_221001_201630.hifi_reads.bc1012_BAK8A_OA--bc1012_BAK8A_OA.bam.pbi',
     'm64089e_221001_201630.primary_qc.tar.xz',
     'm64089e_221001_201630.sts.xml',
     'm64089e_221001_201630.unbarcoded.consensusreadset.xml',
     'm64089e_221001_201630.unbarcoded.hifi_reads.bam',
     'm64089e_221001_201630.unbarcoded.hifi_reads.bam.pbi',
     'm64089e_221001_201630.zmw_metrics.json.gz',
     'merged_analysis_report.json',);

  my @expected_paths;
  push @expected_paths, @expected_pathsA, @expected_pathsB;

  my @observed_paths = observed_data_objects($irods, $dest_coll);

  cmp_ok($num_processed, '==', scalar @expected_paths,
      "Published on instrument files correctly");
  cmp_ok($num_errors,    '==', 1);

  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named on instrument files') or
            diag explain \@observed_paths;
}

sub publish_files_on_instrument_4 : Test(3) {
  ## on instrument CCS - CCS + subreads bam files produced, deplex off instrument
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r64094e_20221214_160714";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r64094e_20221214_160714');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A01") or die "Chmod 0770 directory failed : $!";  

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     runfolder_path  => $tmprf_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  my @expected_paths =
    map { catfile("$dest_coll/1_A01", $_) }
    ('m64094e_221214_161800.consensusreadset.xml',
     'm64094e_221214_161800.primary_qc.tar.xz',
     'm64094e_221214_161800.reads.bam',
     'm64094e_221214_161800.reads.bam.pbi',
     'm64094e_221214_161800.sts.xml',
     'm64094e_221214_161800.subreads.bam',
     'm64094e_221214_161800.subreads.bam.pbi',
     'm64094e_221214_161800.zmw_metrics.json.gz',);

  my @observed_paths = observed_data_objects($irods, $dest_coll);

  cmp_ok($num_processed, '==', scalar @expected_paths,
      "Published on instrument files correctly");
  cmp_ok($num_errors,    '==', 0);

  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named on instrument files') or
            diag explain \@observed_paths;
}

sub publish_files_off_instrument : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $client = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r54097_20170727_165601');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A02") or die "Chmod 0770 directory failed : $!";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $tmprf_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.primary_qc.tar.xz',
     'm54097_170727_170646.scraps.bam',
     'm54097_170727_170646.scraps.bam.pbi',
     'm54097_170727_170646.sts.xml',
     'm54097_170727_170646.subreads.bam',
     'm54097_170727_170646.subreads.bam.pbi',
     'm54097_170727_170646.subreadset.xml');

  cmp_ok($num_processed, '==', scalar @expected_paths,
    "Published off instrument files correctly");
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named off instrument files') or
              diag explain \@observed_paths;

  unlink $pub->restart_file;
}

sub publish_only_runfolder_writable : Test(6) {
  my $irods   = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $client  = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new();

  my $tpath = "$data_path/r64174e_20210114_161659";

  my $tmpdir  = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $trunfolder_path = catdir($tmpdir->dirname, 'r64174e_20210114_161659');
  my $tdata_path = "$trunfolder_path/1_A01";
  dircopy($tpath,$trunfolder_path);
  chmod (0700, $trunfolder_path) or die "Chmod directory $trunfolder_path failed : $!";
  chmod (0700, $tdata_path) or die "Chmod directory $tdata_path failed : $!";

  my $dest_coll = "$irods_tmp_coll/publish_runfolder_writable";
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($trunfolder_path, 'published.json'),
     runfolder_path  => $trunfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  cmp_ok($num_files,     '==', 0);
  cmp_ok($num_processed, '==', 0);
  cmp_ok($num_errors,    '==', 0);

  chmod (0770, $tdata_path) or die "Chmod directory $tdata_path failed : $!";

  my ($num_files2, $num_processed2, $num_errors2) = $pub->publish_files;
  cmp_ok($num_files2,     '==', 5);
  cmp_ok($num_processed2, '==', 5);
  cmp_ok($num_errors2,    '==', 0);

  unlink $pub->restart_file;
}

sub publish_xml_files : Test(14) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmprf_path = catdir($tmpdir->dirname, 'r54097_20170727_165601');
  dircopy($runfolder_path,$tmprf_path) or die $!;
  chmod (0770, "$tmprf_path/1_A02") or die "Chmod 0770 directory failed : $!";

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $tmprf_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.sts.xml',
     'm54097_170727_170646.subreadset.xml');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_xml_files('1_A02', 'subreadset|sts',2);
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named metadata XML files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

  unlink $pub->restart_file;
}

sub publish_aux_files : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_adapter_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.adapters.fasta');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_aux_files('1_A02','adapters[.]fasta$');
  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named aux files') or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

  unlink $pub->restart_file;
}

sub publish_sequence_files : Test(40) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my $pattern  = $FILE_PREFIX_PATTERN .'[.]'. q[subreadset] .'[.]xml$';
  my $metafile = $pub->list_files('1_A02', $pattern, '1')->[0];
  my $meta = WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file($metafile);

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.scraps.bam',
     'm54097_170727_170646.subreads.bam');

  my ($num_files1, $num_processed1, $num_errors1) =
    $pub->publish_sequence_files('1_A02',$SEQUENCE_PRODUCT,$meta);
  my ($num_files2, $num_processed2, $num_errors2) =
    $pub->publish_sequence_files('1_A02',$SEQUENCE_AUXILIARY,$meta);

  cmp_ok($num_files1 + $num_files2, '==', scalar @expected_paths);
  cmp_ok($num_processed1 + $num_processed2, '==', scalar @expected_paths);
  cmp_ok($num_errors1 + $num_errors2, '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named sequence files') or
              diag explain \@observed_paths;

  check_primary_metadata($irods, $pub, @observed_paths);
  check_common_metadata($irods, @observed_paths);
  check_study_metadata($irods, @observed_paths);


  unlink $pub->restart_file;
}

sub publish_index_files : Test(14) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_sequence_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.scraps.bam.pbi',
     'm54097_170727_170646.subreads.bam.pbi');
  
  my $seq_types = qq{($SEQUENCE_PRODUCT|$SEQUENCE_AUXILIARY)};
  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_index_files('1_A02', $seq_types);
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

sub publish_image_archive : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/r54097_20170727_165601";
  my $dest_coll = "$irods_tmp_coll/publish_sequence_files";

  my $client = TestAPIClient->new(default_interval => 10000,);
  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");

  my $pub = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new
    (api_client      => $client,
     dest_collection => $dest_coll,
     irods           => $irods,
     mlwh_schema     => $wh_schema,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my $pattern  = $FILE_PREFIX_PATTERN .'[.]'. q[subreadset] .'[.]xml$';
  my $metafile = $pub->list_files('1_A02', $pattern, '1')->[0];
  my $meta = WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file($metafile);

  my @expected_paths =
    map { catfile("$dest_coll/1_A02", $_) }
    ('m54097_170727_170646.primary_qc.tar.xz');

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_image_archive('1_A02', $meta,
    $WTSI::NPG::HTS::PacBio::Sequel::RunPublisher::OFFINSTRUMENT);

  cmp_ok($num_files,     '==', scalar @expected_paths);
  cmp_ok($num_processed, '==', scalar @expected_paths);
  cmp_ok($num_errors,    '==', 0);

  my @observed_paths = observed_data_objects($irods, $dest_coll);
  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named image archive file') or
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
  my ($irods, $pub, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr
      ($PACBIO_CELL_INDEX,
       $PACBIO_COLLECTION_NUMBER,
       $PACBIO_DATA_LEVEL,
       $PACBIO_INSTRUMENT_NAME,
       $PACBIO_RUN,
       $PACBIO_WELL,
       $PACBIO_SAMPLE_LOAD_NAME,
       $ID_PRODUCT) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }

    my @runs = $obj->find_in_metadata($PACBIO_RUN);
    my $run_name = $runs[0]->{value};
    my @wells = $obj->find_in_metadata($PACBIO_WELL);
    my $well_label = $pub->remove_well_padding($run_name, $wells[0]->{value});
    my @product_ids  = $obj->find_in_metadata($ID_PRODUCT);
    my $product_id = $product_ids[0]->{value};

    my $product = WTSI::NPG::HTS::PacBio::Sequel::Product->new();
    my $expected_id;
    if ($obj->find_in_metadata($TARGET)){
      my @tags = $obj->find_in_metadata($TAG_SEQUENCE);
      my $tags;
      foreach my $tag (@tags){
        if (defined($tags)) {
          $tags = join(q/,/, $tags, $tag->{value});
        } else {
          $tags = $tag->{value};
        }
      }
      $expected_id = $product->generate_product_id($run_name, $well_label, $tags);
    }else{
      $expected_id = $product->generate_product_id($run_name, $well_label);
    }

    is($product_id, $expected_id,
      "$file_name has expected id_product metadata");
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

sub read_json_content {
  my ($path) = @_;

  open my $mlwh_json_fh, '<:encoding(UTF-8)', $path or die qq[could not open $path];

  my $json = decode_json(<$mlwh_json_fh>);
  close $mlwh_json_fh;
  return $json
}

sub set_destination {
  my ($json_hash, $temp_coll) = @_;
  foreach my $product (@{$json_hash->{products}}){
    $product->{irods_root_collection} =~ s|/testZone/home/irods/RunPublisherTest.XXXXX.0/|$temp_coll/|xms;
  }
  return $json_hash;
}

1;
