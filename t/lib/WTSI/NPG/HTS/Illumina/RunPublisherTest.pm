package WTSI::NPG::HTS::Illumina::RunPublisherTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel catfile catdir splitdir];
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::HTS::Illumina::RunPublisher;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::iRODS::DataObject;
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
my $data_path    = 't/data/illumina';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $wh_schema;
my $lims_factory;

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
    $irods->add_collection("RunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

# Run-level data files
sub publish_interop_files : Test(45) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # HiSeq
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $id_run         = 18448;
  my $dest_coll      = "$irods_tmp_coll/publish_interop_files";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_interop_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 7, 'Published 7 InterOp files');

  my @observed = observed_data_objects($irods, "$dest_coll/InterOp",
                                       $dest_coll, '[.]bin$');
  my @expected = qw[InterOp/ControlMetricsOut.bin
                    InterOp/CorrectedIntMetricsOut.bin
                    InterOp/ErrorMetricsOut.bin
                    InterOp/ExtractionMetricsOut.bin
                    InterOp/ImageMetricsOut.bin
                    InterOp/QMetricsOut.bin
                    InterOp/TileMetricsOut.bin];
  deep_observed_vs_expected(\@observed, \@expected,
                            "Published correctly named InterOp files");

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::InterOpDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  foreach my $path (@absolute_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::InterOpDataObject->new($irods, $path);
    is_deeply($obj->get_avu($ID_RUN), { attribute => $ID_RUN,
                                        value     => $id_run },
           "$path id_run metadata present");
  }
}

sub publish_interop_files_deep : Test(87) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  # NovaSeq
  my $runfolder_path = "$data_path/sequence/180709_A00538_0010_BH3FCMDRXX";
  my $id_run         = 26291;
  my $dest_coll      = "$irods_tmp_coll/publish_interop_files";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_interop_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 14, 'Published 14 InterOp files');

  my @observed = observed_data_objects($irods, "$dest_coll/InterOp",
                                       $dest_coll, '[.]bin$');
  my @expected = qw[InterOp/C1.1/ControlMetricsOut.bin
                    InterOp/C1.1/CorrectedIntMetricsOut.bin
                    InterOp/C1.1/ErrorMetricsOut.bin
                    InterOp/C1.1/ExtractionMetricsOut.bin
                    InterOp/C1.1/ImageMetricsOut.bin
                    InterOp/C1.1/QMetricsOut.bin
                    InterOp/C1.1/TileMetricsOut.bin
                    InterOp/C2.1/ControlMetricsOut.bin
                    InterOp/C2.1/CorrectedIntMetricsOut.bin
                    InterOp/C2.1/ErrorMetricsOut.bin
                    InterOp/C2.1/ExtractionMetricsOut.bin
                    InterOp/C2.1/ImageMetricsOut.bin
                    InterOp/C2.1/QMetricsOut.bin
                    InterOp/C2.1/TileMetricsOut.bin];
  deep_observed_vs_expected(\@observed, \@expected,
                            "Published correctly named InterOp files");

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::InterOpDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  foreach my $path (@absolute_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::InterOpDataObject->new($irods, $path);
    is_deeply($obj->get_avu($ID_RUN), { attribute => $ID_RUN,
                                        value     => $id_run },
           "$path id_run metadata present");
  }
}

sub publish_xml_files : Test(18) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $id_run         = 18448;
  my $dest_coll      = "$irods_tmp_coll/publish_xml_files";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_xml_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 2, 'Published 2 XML files');

  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll,
                                       '[.]xml$');
  my @expected = ('RunInfo.xml', 'runParameters.xml');
  is_deeply(\@observed, \@expected, 'Published correctly named XML files') or
    diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::XMLDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  foreach my $path (@absolute_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::XMLDataObject->new($irods, $path);
    is_deeply($obj->get_avu($ID_RUN), { attribute => $ID_RUN,
                                        value     => $id_run },
              "$path id_run metadata present");
  }

  my $source_dir = catdir($db_dir, '18448_runfolder');
  mkdir $source_dir;
  open my $fh, '>', catfile($source_dir, 'RunParameters.xml')
    or die 'Failed to create a test file';
  print $fh '<runparams>some info</runparams>'
    or die 'Failed to write to a test file';
  close $fh;

  $dest_coll = "$irods_tmp_coll/publish_xml_files2";

  $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published2.json'),
     source_directory => $source_dir);
  ($num_files, $num_processed, $num_errors) = $pub->publish_xml_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 1, 'Published 1 XML file');

  @observed = observed_data_objects($irods, $dest_coll, $dest_coll,
                                    '[.]xml$');
  @expected = ('RunParameters.xml');
  is_deeply(\@observed, \@expected, 'Published correctly named XML files') or
    diag explain \@observed;
}

sub publish_qc_files : Test(99) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $dest_coll      = "$irods_tmp_coll/publish_qc_files";
  my $id_run         = 18448;
  my $lane           = 2;
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $archive_path);

  my $name = sprintf q[%s_%s], $id_run, $lane;
  my $composition_file = "$archive_path/$name.composition.json";
  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_qc_files($composition_file);
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 16, 'Published 16 QC files');

  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll,
                                       '[.]json$');
  my @expected = ('qc/18448_2.adapter.json',
                  'qc/18448_2.alignment_filter_metrics.json',
                  'qc/18448_2.bam_flagstats.json',
                  'qc/18448_2.gc_bias.json',
                  'qc/18448_2.gc_fraction.json',
                  'qc/18448_2.genotype.json',
                  'qc/18448_2.insert_size.json',
                  'qc/18448_2.pulldown_metrics.json',
                  'qc/18448_2.qX_yield.json',
                  'qc/18448_2.ref_match.json',
                  'qc/18448_2.sequence_error.json',
                  'qc/18448_2.sequence_summary.json',
                  'qc/18448_2.spatial_filter.json',
                  'qc/18448_2.verify_bam_id.json',
                  'qc/18448_2_F0x900.samtools_stats.json',
                  'qc/18448_2_F0xB00.samtools_stats.json');
  is_deeply(\@observed, \@expected, 'Published correctly named QC files') or
    diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
  check_study_id_metadata($irods, $pkg, @absolute_paths);
}

# Lane-level, primary and secondary data, from ML warehouse
sub publish_lane_pri_data_mlwh : Test(21) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;
  my $lane           = 2;
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
  my $dest_coll = check_publish_lane_pri_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ('18448_2.cram');
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
  check_primary_metadata($irods, $pkg, @absolute_paths);
  check_study_id_metadata($irods, $pkg, @absolute_paths);
  check_study_metadata($irods, $pkg, @absolute_paths);
}

sub publish_lane_sec_data_mlwh : Test(79) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;
  my $lane           = 2;
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll = check_publish_lane_sec_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ('18448_2.all.seqchksum',
                  '18448_2.bam_stats',
                  '18448_2.composition.json',
                  '18448_2.cram.crai',
                  '18448_2.flagstat',
                  '18448_2.geno',
                  '18448_2.markdups_metrics.txt',
                  '18448_2.seqchksum',
                  '18448_2.sha512primesums512.seqchksum',
                  '18448_2_F0x900.stats',
                  '18448_2_F0xB00.stats',
                  '18448_2_quality_cycle_caltable.txt',
                  '18448_2_quality_cycle_surv.txt',
                  '18448_2_quality_error.txt',
                  '18448_2_salmon.quant.zip');
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

# Lane-level, primary and secondary data, from samplesheet
sub publish_lane_pri_data_samplesheet : Test(21) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;
  my $lane           = 2;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20151214-085833/" .
    "metadata_cache_18448/samplesheet_18448.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');
  my $dest_coll = check_publish_lane_pri_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ('18448_2.cram');

  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
  check_primary_metadata($irods, $pkg, @absolute_paths);
  check_study_id_metadata($irods, $pkg, @absolute_paths);
  check_study_metadata($irods, $pkg, @absolute_paths);
}

sub publish_lane_sec_data_samplesheet : Test(79) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;
  my $lane           = 2;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20151214-085833/" .
    "metadata_cache_18448/samplesheet_18448.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');
  my $dest_coll = check_publish_lane_sec_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ('18448_2.all.seqchksum',
                  '18448_2.bam_stats',
                  '18448_2.composition.json',
                  '18448_2.cram.crai',
                  '18448_2.flagstat',
                  '18448_2.geno',
                  '18448_2.markdups_metrics.txt',
                  '18448_2.seqchksum',
                  '18448_2.sha512primesums512.seqchksum',
                  '18448_2_F0x900.stats',
                  '18448_2_F0xB00.stats',
                  '18448_2_quality_cycle_caltable.txt',
                  '18448_2_quality_cycle_surv.txt',
                  '18448_2_quality_error.txt',
                  '18448_2_salmon.quant.zip');
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

# Plex-level, primary and secondary data, from ML warehouse
sub publish_plex_pri_data_mlwh : Test(21) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $id_run         = 17550;
  my $lane           = 1;
  my $plex           = 1;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll = check_publish_plex_pri_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $plex,
                                              $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ("lane$lane/17550_1#1.cram");
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
  check_primary_metadata($irods, $pkg, @absolute_paths);
  check_study_id_metadata($irods, $pkg, @absolute_paths);
  check_study_metadata($irods, $pkg, @absolute_paths);
}

sub publish_plex_sec_data_mlwh : Test(59) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $id_run         = 17550;
  my $lane           = 1;
  my $plex           = 1;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll = check_publish_plex_sec_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $plex,
                                              $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  # No index files expected because the CRAM file contains no reads
  my @expected = ("lane$lane/17550_1#1.bam_stats",
                  "lane$lane/17550_1#1.composition.json",
                  "lane$lane/17550_1#1.flagstat",
                  "lane$lane/17550_1#1.markdups_metrics.txt",
                  "lane$lane/17550_1#1.seqchksum",
                  "lane$lane/17550_1#1.sha512primesums512.seqchksum",
                  "lane$lane/17550_1#1_F0x900.stats",
                  "lane$lane/17550_1#1_F0xB00.stats",
                  "lane$lane/17550_1#1_quality_cycle_caltable.txt",
                  "lane$lane/17550_1#1_quality_cycle_surv.txt",
                  "lane$lane/17550_1#1_quality_error.txt");
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

# Plex-level, primary and secondary data, from samplesheet
sub publish_plex_pri_data_samplesheet : Test(21) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $id_run         = 17550;
  my $lane           = 1;
  my $plex           = 1;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/" .
    "metadata_cache_17550/samplesheet_17550.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  my $dest_coll = check_publish_plex_pri_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $plex,
                                              $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, "$dest_coll/lane$lane",
                                       $dest_coll);
  my @expected = ("lane$lane/17550_1#1.cram");
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
  check_primary_metadata($irods, $pkg, @absolute_paths);
  check_study_id_metadata($irods, $pkg, @absolute_paths);
  check_study_metadata($irods, $pkg, @absolute_paths);
}

sub publish_plex_sec_data_samplesheet : Test(59) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $id_run         = 17550;
  my $lane           = 1;
  my $plex           = 1;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/" .
    "metadata_cache_17550/samplesheet_17550.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  my $dest_coll = check_publish_plex_sec_data($runfolder_path, $archive_path,
                                              $id_run, $lane, $plex,
                                              $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  # No index files expected because the CRAM file contains no reads
  my @expected = ("lane$lane/17550_1#1.bam_stats",
                  "lane$lane/17550_1#1.composition.json",
                  "lane$lane/17550_1#1.flagstat",
                  "lane$lane/17550_1#1.markdups_metrics.txt",
                  "lane$lane/17550_1#1.seqchksum",
                  "lane$lane/17550_1#1.sha512primesums512.seqchksum",
                  "lane$lane/17550_1#1_F0x900.stats",
                  "lane$lane/17550_1#1_F0xB00.stats",
                  "lane$lane/17550_1#1_quality_cycle_caltable.txt",
                  "lane$lane/17550_1#1_quality_cycle_surv.txt",
                  "lane$lane/17550_1#1_quality_error.txt");
  is_deeply(\@observed, \@expected) or diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

# Merged NovaSeq data, from ML warehouse
sub publish_merged_pri_data_mlwh : Test(19) {
  my $runfolder_path = "$data_path/sequence/180709_A00538_0010_BH3FCMDRXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20180805-013153/no_cal/archive';
  my $id_run         = 26291;
  my $plex           = 1;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll =
    check_publish_merged_pri_data($runfolder_path, $archive_path,
                                  $id_run, $plex, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  deep_observed_vs_expected([observed_data_objects($irods, $dest_coll,
                                                   $dest_coll)],
                            ["plex$plex/26291#1.cram"],
                            'Expected data object found');

  my @observed = observed_data_objects($irods, "$dest_coll/plex$plex",
                                       $dest_coll);
  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  my $obj = $pkg->new($irods, "$dest_coll/plex$plex/26291#1.cram");
  check_merge_primary_metadata($obj);
}

# Merged NovaSeq data, from samplesheet
sub publish_merged_pri_data_samplesheet : Test(19) {
  my $runfolder_path = "$data_path/sequence/180709_A00538_0010_BH3FCMDRXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20180805-013153/no_cal/archive';
  my $id_run         = 26291;
  my $plex           = 1;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20180805-013153/" .
    "metadata_cache_26291/samplesheet_26291.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  my $dest_coll =
    check_publish_merged_pri_data($runfolder_path, $archive_path,
                                  $id_run, $plex, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  deep_observed_vs_expected([observed_data_objects($irods, $dest_coll,
                                                   $dest_coll)],
                            ["plex$plex/26291#1.cram"],
                            'Expected data object found');

  my @observed = observed_data_objects($irods, "$dest_coll/plex$plex",
                                       $dest_coll);
  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  my $obj = $pkg->new($irods, "$dest_coll/plex$plex/26291#1.cram");
  check_merge_primary_metadata($obj);
}

sub publish_merged_sec_data_mlwh : Test(89) {
  my $runfolder_path = "$data_path/sequence/180709_A00538_0010_BH3FCMDRXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20180805-013153/no_cal/archive';
  my $id_run         = 26291;
  my $plex           = 1;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll =
    check_publish_merged_sec_data($runfolder_path, $archive_path,
                                  $id_run, $plex, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  deep_observed_vs_expected([observed_data_objects($irods,
                                                   "$dest_coll/plex$plex",
                                                   $dest_coll)],
                            ["plex$plex/26291#1.bam_stats",
                             "plex$plex/26291#1.bcfstats",
                             "plex$plex/26291#1.bqsr_table",
                             "plex$plex/26291#1.composition.json",
                             "plex$plex/26291#1.cram.crai",
                             "plex$plex/26291#1.flagstat",
                             "plex$plex/26291#1.g.vcf.gz",
                             "plex$plex/26291#1.g.vcf.gz.tbi",
                             "plex$plex/26291#1.markdups_metrics.txt",
                             "plex$plex/26291#1.orig.seqchksum",
                             "plex$plex/26291#1.seqchksum",
                             "plex$plex/26291#1.sha512primesums512.seqchksum",
                             "plex$plex/26291#1.spatial_filter.stats",
                             "plex$plex/26291#1_F0x900.stats",
                             "plex$plex/26291#1_F0xB00.stats",
                             "plex$plex/26291#1_F0xF04_target.stats",
                             "plex$plex/26291#1_F0xF04_target_autosome.stats"],
                            'Expected data object found');

  my @observed = observed_data_objects($irods, "$dest_coll/plex$plex",
                                       $dest_coll);
  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

sub publish_merged_sec_data_samplesheet : Test(89) {
  my $runfolder_path = "$data_path/sequence/180709_A00538_0010_BH3FCMDRXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20180805-013153/no_cal/archive';
  my $id_run         = 26291;
  my $plex           = 1;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    "$runfolder_path/Data/Intensities/BAM_basecalls_20180805-013153/" .
    "metadata_cache_26291/samplesheet_26291.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  my $dest_coll =
    check_publish_merged_sec_data($runfolder_path, $archive_path,
                                  $id_run, $plex, $lims_factory);

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  deep_observed_vs_expected([observed_data_objects($irods,
                                                   "$dest_coll/plex$plex",
                                                   $dest_coll)],
                            ["plex$plex/26291#1.bam_stats",
                             "plex$plex/26291#1.bcfstats",
                             "plex$plex/26291#1.bqsr_table",
                             "plex$plex/26291#1.composition.json",
                             "plex$plex/26291#1.cram.crai",
                             "plex$plex/26291#1.flagstat",
                             "plex$plex/26291#1.g.vcf.gz",
                             "plex$plex/26291#1.g.vcf.gz.tbi",
                             "plex$plex/26291#1.markdups_metrics.txt",
                             "plex$plex/26291#1.orig.seqchksum",
                             "plex$plex/26291#1.seqchksum",
                             "plex$plex/26291#1.sha512primesums512.seqchksum",
                             "plex$plex/26291#1.spatial_filter.stats",
                             "plex$plex/26291#1_F0x900.stats",
                             "plex$plex/26291#1_F0xB00.stats", 
                             "plex$plex/26291#1_F0xF04_target.stats",
                             "plex$plex/26291#1_F0xF04_target_autosome.stats"],
                            'Expected data object found');

  my @observed = observed_data_objects($irods, "$dest_coll/plex$plex",
                                       $dest_coll);
  my @absolute_paths = map { "$dest_coll/$_" } @observed;
  my $pkg = 'WTSI::NPG::HTS::Illumina::AncDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);
}

sub publish_plex_pri_data_alt_process : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $id_run         = 17550;
  my $lane           = 1;
  my $plex           = 1;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $name = sprintf q[%s_%s#%s], $id_run, $lane, $plex;
  my $composition_file = sprintf q[%s/lane%s/%s.composition.json],
    $archive_path, $lane, $name;
  my $alt_process = 'an_alternative_process';
  my $coll = 'publish_alt_process';

  my $dest_coll =
    check_publish_pri_data($runfolder_path, $archive_path, $lims_factory,
                           $composition_file, $coll, $alt_process);

  my @path = grep { length } splitdir($dest_coll);
  is($path[-1], $alt_process, 'Expected leaf collection present')
    or diag explain $dest_coll;

  my @observed = observed_data_objects($irods, "$dest_coll/lane$lane",
                                       $dest_coll);
  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::AlnDataObject';
  check_alt_process_metadata($irods, $pkg, $alt_process, @absolute_paths);
}

sub publish_xml_files_alt_process : Test(15) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $id_run         = 18448;
  my $dest_coll      = "$irods_tmp_coll/publish_xml_files";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
  my $alt_process = 'an_alternative_process';

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (alt_process      => $alt_process,
     id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_xml_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 2, 'Published 2 XML files');

  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll,
                                       '[.]xml$');
  my @expected = ("$alt_process/RunInfo.xml",
                  "$alt_process/runParameters.xml");
  is_deeply(\@observed, \@expected, 'Published correctly named XML files') or
    diag explain \@observed;

  my @absolute_paths = map { "$dest_coll/$_" } @observed;

  my $pkg = 'WTSI::NPG::HTS::Illumina::XMLDataObject';
  check_common_metadata($irods, $pkg, @absolute_paths);

  foreach my $path (@absolute_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::XMLDataObject->new($irods, $path);
    is_deeply($obj->get_avu($ID_RUN), { attribute => $ID_RUN,
                                        value     => $id_run },
              "$path id_run metadata present");
  }
}

sub publish_include_exclude : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;
  my $dest_coll      = "$irods_tmp_coll/publish_include";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $archive_path,
     include          => ['\/18448_2'],
     exclude          => ['phix']);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 32, 'Published 32 files');

  my @observed = observed_data_objects($irods, $dest_coll, $dest_coll);
  my @expected = ('18448_2.all.seqchksum',
                  '18448_2.bam_stats',
                  '18448_2.composition.json',
                  '18448_2.cram',
                  '18448_2.cram.crai',
                  '18448_2.flagstat',
                  '18448_2.geno',
                  '18448_2.markdups_metrics.txt',
                  '18448_2.seqchksum',
                  '18448_2.sha512primesums512.seqchksum',
                  '18448_2_F0x900.stats',
                  '18448_2_F0xB00.stats',
                  '18448_2_quality_cycle_caltable.txt',
                  '18448_2_quality_cycle_surv.txt',
                  '18448_2_quality_error.txt',
                  '18448_2_salmon.quant.zip',
                  'qc/18448_2.adapter.json',
                  'qc/18448_2.alignment_filter_metrics.json',
                  'qc/18448_2.bam_flagstats.json',
                  'qc/18448_2.gc_bias.json',
                  'qc/18448_2.gc_fraction.json',
                  'qc/18448_2.genotype.json',
                  'qc/18448_2.insert_size.json',
                  'qc/18448_2.pulldown_metrics.json',
                  'qc/18448_2.qX_yield.json',
                  'qc/18448_2.ref_match.json',
                  'qc/18448_2.sequence_error.json',
                  'qc/18448_2.sequence_summary.json',
                  'qc/18448_2.spatial_filter.json',
                  'qc/18448_2.verify_bam_id.json',
                  'qc/18448_2_F0x900.samtools_stats.json',
                  'qc/18448_2_F0xB00.samtools_stats.json');
  is_deeply(\@observed, \@expected) or diag explain \@observed;
}

sub publish_archive_path_mlwh : Test(6) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $id_run         = 18448;

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $dest_coll = "$irods_tmp_coll/publish_entire_mlwh";

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

  my $num_expected = 379;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', $num_expected, "Published $num_expected files");

  my $restart_file = $pub->restart_file;
  ok(-e $restart_file, "Restart file $restart_file was written by publisher");

  my $restart_state = WTSI::NPG::HTS::PublishState->new;
  $restart_state->read_state($restart_file);
  cmp_ok($restart_state->num_published, '==', $num_files,
         "Restart file recorded $num_files files published") or
           diag explain $restart_state->state;

  my $repub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (id_run           => $id_run,
     dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $runfolder_path);

  my ($repub_files, $repub_processed, $repub_errors) = $repub->publish_files;
  cmp_ok($repub_errors,    '==', 0, 'No errors on re-publishing');
  cmp_ok($repub_processed, '==', 0, "Re-published no files");
}

# From here onwards are test support functions

sub check_publish_lane_pri_data {
  my ($runfolder_path, $archive_path, $id_run, $lane, $lims_factory) = @_;

  my $coll = 'publish_lane_pri_data';
  my $name = sprintf q[%s_%s], $id_run, $lane;
  my $composition_file = "$archive_path/$name.composition.json";

  return check_publish_pri_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_lane_sec_data {
  my ($runfolder_path, $archive_path, $id_run, $lane, $lims_factory) = @_;

  my $coll = 'publish_lane_sec_data';
  my $name = sprintf q[%s_%s], $id_run, $lane;
  my $composition_file = "$archive_path/$name.composition.json";

  return check_publish_sec_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_plex_pri_data {
  my ($runfolder_path, $archive_path, $id_run, $lane, $plex,
      $lims_factory) = @_;

  my $coll = 'publish_plex_pri_data';
  my $name = sprintf q[%s_%s#%s], $id_run, $lane, $plex;
  my $composition_file = sprintf q[%s/lane%s/%s.composition.json],
    $archive_path, $lane, $name;

  return check_publish_pri_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_plex_sec_data {
  my ($runfolder_path, $archive_path, $id_run, $lane, $plex,
      $lims_factory) = @_;

  my $coll = 'publish_plex_sec_data';
  my $name = sprintf q[%s_%s#%s], $id_run, $lane, $plex;
  my $composition_file = sprintf q[%s/lane%s/%s.composition.json],
    $archive_path, $lane, $name;

  return check_publish_sec_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_merged_pri_data {
  my ($runfolder_path, $archive_path, $id_run, $plex, $lims_factory) = @_;

  my $coll = 'publish_merged_pri_data';
  my $name = sprintf q[%s#%s], $id_run, $plex;
  my $composition_file = sprintf q[%s/plex%s/%s.composition.json],
    $archive_path, $plex, $name;

  return check_publish_pri_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_merged_sec_data {
  my ($runfolder_path, $archive_path, $id_run, $plex, $lims_factory) = @_;

  my $coll = 'publish_merged_sec_data';
  my $name = sprintf q[%s#%s], $id_run, $plex;
  my $composition_file = sprintf q[%s/plex%s/%s.composition.json],
    $archive_path, $plex, $name;

  return check_publish_sec_data($runfolder_path, $archive_path, $lims_factory,
                                $composition_file, $coll);
}

sub check_publish_pri_data {
  my ($runfolder_path, $archive_path, $lims_factory,
      $composition_file, $coll, $alt_process) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $dest_coll = "$irods_tmp_coll/$coll";
  my $publish_coll = $dest_coll;

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");

  my @init_args =
    (dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $archive_path);

  if (defined $alt_process) {
    push @init_args, $ALT_PROCESS => $alt_process;
    $publish_coll = "$dest_coll/$alt_process";
  }

  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new(@init_args);

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_alignment_files($composition_file, $archive_path);

  cmp_ok($num_errors, '==', 0, 'No errors on publishing');

  my $restart_file = $pub->restart_file;
  $pub->write_restart_file;
  ok(-e $restart_file, "Restart file $restart_file exists");

  my $restart_state = WTSI::NPG::HTS::PublishState->new;
  $restart_state->read_state($restart_file);
  cmp_ok($restart_state->num_published, '==', $num_files,
         "Restart file recorded $num_files files published");

  return $publish_coll;
}

sub check_publish_sec_data {
  my ($runfolder_path, $archive_path, $lims_factory,
      $composition_file, $coll) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $dest_coll = "$irods_tmp_coll/$coll";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection  => $dest_coll,
     irods            => $irods,
     lims_factory     => $lims_factory,
     restart_file     => catfile($tmpdir->dirname, 'published.json'),
     source_directory => $archive_path);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);
  my ($nf0, $np0, $ne0) = $pub->publish_ancillary_files($composition_file);
  my ($nf1, $np1, $ne1) = $pub->publish_index_files($composition_file);
  my ($nf2, $np2, $ne2) = $pub->publish_genotype_files($composition_file);

  $num_files     = $nf0 + $nf1 + $nf2;
  $num_processed = $np0 + $np1 + $np2;
  $num_errors    = $ne0 + $ne1 + $ne2;

  cmp_ok($num_errors, '==', 0, 'No errors on publishing');

  my $restart_file = $pub->restart_file;
  $pub->write_restart_file;
  ok(-e $restart_file, "Restart file $restart_file exists");

  my $restart_state = WTSI::NPG::HTS::PublishState->new;
  $restart_state->read_state($restart_file);
  cmp_ok($restart_state->num_published, '==', $num_files,
         "Restart file recorded $num_files files published");

  return $dest_coll;
}

sub check_common_metadata {
  my ($irods, $pkg, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = $pkg->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($DCTERMS_CREATED, $DCTERMS_CREATOR, $DCTERMS_PUBLISHER,
                      $FILE_TYPE, $FILE_MD5) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_primary_metadata {
  my ($irods, $pkg, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = $pkg->new($irods, $path);
    my $file_name = fileparse($obj->str);

    foreach my $attr ($ALIGNMENT, $ID_RUN, $POSITION, $COMPOSITION,
                      $TOTAL_READS, $IS_PAIRED_READ, $SEQCHKSUM, $TARGET) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_merge_primary_metadata {
  my ($obj) = @_;

  is(26291, $obj->id_run, 'Expected id_run');

  deep_observed_vs_expected
    ([$obj->find_in_metadata($COMPOSITION)],
     [{attribute => $COMPOSITION,
       value     =>
       '{"components":[{"id_run":26291,"position":1,"tag_index":1},' .
                      '{"id_run":26291,"position":2,"tag_index":1}]}'}],
     'Expected composition AVU present');

  deep_observed_vs_expected
    ([$obj->find_in_metadata($COMPONENT)],
     [{attribute => $COMPONENT,
       value     => '{"id_run":26291,"position":1,"tag_index":1}'},
      {attribute => $COMPONENT,
       value     => '{"id_run":26291,"position":2,"tag_index":1}'}],
     'Expected component AVUs present');

  deep_observed_vs_expected
    ([$obj->find_in_metadata($ID_RUN)],
     [{attribute => $ID_RUN,
       value     => 26291}],
     'Expected id_run AVU present');

  deep_observed_vs_expected
    ([$obj->find_in_metadata($POSITION)],
     [],
     'Lane AVUs are not present - components belong to different lanes');

  foreach my $attr ($ALIGNMENT, $TOTAL_READS, $IS_PAIRED_READ,
                    $SEQCHKSUM, $TARGET) {
    my @avu = $obj->find_in_metadata($attr);
    cmp_ok(scalar @avu, '==', 1, "Expected $attr metadata present");
  }
}

sub check_study_id_metadata {
  my ($irods, $pkg, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = $pkg->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my @avu = $obj->find_in_metadata($STUDY_ID);
    cmp_ok(scalar @avu, '>=', 1, "$file_name $STUDY_ID metadata present");
  }
}

# Calls tag_index, so only works for unmerged data
sub check_study_metadata {
  my ($irods, $pkg, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = $pkg->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my $tag_index = $obj->tag_index;

    # Tag 888 has no study accession or name
    if ($tag_index and $tag_index == 888) {
      foreach my $attr ($STUDY_ID, $STUDY_NAME) {
        my @avu = $obj->find_in_metadata($attr);
        cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
      }
      foreach my $attr ($STUDY_ACCESSION_NUMBER) {
        my @avu = $obj->find_in_metadata($attr);
        cmp_ok(scalar @avu, '==', 0, "$file_name $attr metadata absent");
      }
    }
    else {
      foreach my $attr ($STUDY_ID, $STUDY_NAME, $STUDY_ACCESSION_NUMBER) {
        my @avu = $obj->find_in_metadata($attr);
        cmp_ok(scalar @avu, '>=', 1, "$file_name $attr metadata present");
      }
    }

    # Not testing study description because these may be removed
  }
}

sub check_alt_process_metadata {
  my ($irods, $pkg, $alt_process, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = $pkg->new($irods, $path);
    my $file_name = fileparse($obj->str);

    is_deeply([$obj->get_avu($TARGET)],
              [{attribute => $TARGET,
                value     => 0}],
              "$file_name $TARGET metadata correct when alt_process");
    is_deeply([$obj->get_avu($ALT_TARGET)],
              [{attribute => $ALT_TARGET,
                value     => 1}],
              "$file_name $ALT_TARGET metadata correct when alt_process");
    is_deeply([$obj->get_avu($ALT_PROCESS)],
              [{attribute => $ALT_PROCESS,
                value     => $alt_process}],
              "$file_name $ALT_PROCESS metadata correct when alt_process");
  }
}

sub calc_lane_alignment_files {
  my ($root_path, $id_run) = @_;

  my %position_index;

  my $file_format = 'cram';

  foreach my $position (1 .. 8) {
    my @lane_files;
    push @lane_files, sprintf '%s/%d_%d.%s',
      $root_path, $id_run, $position, $file_format;
    push @lane_files, sprintf '%s/%d_%d_phix.%s',
      $root_path, $id_run, $position, $file_format;

    @lane_files = sort @lane_files;
    $position_index{$position} = \@lane_files;
  }

  return %position_index;
}

sub expected_data_objects {
  my ($dest_collection, $position_index, $position) = @_;

  my @expected_paths = map {
    catfile($dest_collection, scalar fileparse($_))
  } @{$position_index->{$position}};
  @expected_paths = sort @expected_paths;

  return @expected_paths;
}

sub observed_data_objects {
  my ($irods, $dest_collection, $root_collection, $regex) = @_;

  my ($observed_paths) = $irods->list_collection($root_collection, 'RECURSE');
  my @observed_paths = @{$observed_paths};
  if ($regex) {
    @observed_paths = grep { m{$regex}msx } @observed_paths;
  }
  @observed_paths = sort @observed_paths;
  @observed_paths = map { abs2rel($_, $root_collection) } @observed_paths;

  return @observed_paths;
}

sub deep_observed_vs_expected {
  my ($observed, $expected, $message) = @_;

  is_deeply($observed, $expected, $message) or diag explain $observed;
}

1;
