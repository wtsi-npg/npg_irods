package WTSI::NPG::HTS::Illumina::RunPublisherTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::HTS::Illumina::AncDataObject;
use WTSI::NPG::HTS::Illumina::RunPublisher;
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
my $data_path    = 't/data/run_publisher';
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

sub positions : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    is_deeply([$pub->positions], [1 .. 8],
              "Found expected positions ($file_format)")
      or diag explain $pub->positions;
  }
}

sub num_reads : Test(102) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  # Lane-level
  my $lane_runfolder_path =
    "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";

  my $lane_expected_read_counts = [0,        # edited from 863061182
                                   856966676,
                                   898136862,
                                   893691470,
                                   869960390,
                                   894014820,
                                   883399526,
                                   899795972];

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $lane_runfolder_path);

    foreach my $lane_position (1 .. 8) {
      my $expected = $lane_expected_read_counts->[$lane_position - 1];
      my $count    = $pub->num_reads($lane_position);
      cmp_ok($count, '==', $expected,
             "num_reads for $file_format position $lane_position") or
               diag explain $count;
    }
  }

  # Lane-level alignment filter
  my $lane_phix_expected_read_counts = [13311044,
                                        12289198,
                                        14731576,
                                        13273222,
                                        12145720,
                                        13836878,
                                        13586652,
                                        13564700];
  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $lane_runfolder_path);

    foreach my $lane_position (1 .. 8) {
      my $expected = $lane_phix_expected_read_counts->[$lane_position - 1];
      my $count    = $pub->num_reads($lane_position,
                                     alignment_filter => 'phix');
      cmp_ok($count, '==', $expected,
             "num_reads for $file_format position $lane_position phix") or
               diag explain $count;
    }
  }

  # Plex-level
  my $plex_runfolder_path =
    "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  my $plex_position = 1;
  my $tag_count     = 16,
  my $plex_expected_read_counts = [3334934,  # tag 0
                                   0,        # edited from 71488156,
                                   29817458, 15354480, 33948370,
                                   33430552, 24094786, 32604688, 26749430,
                                   27668866, 30775624, 33480806, 40965140,
                                   32087634, 37315470, 27193418, 31538878,
                                   1757876]; # tag 888

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $plex_runfolder_path);

    my @tags = (0 .. $tag_count, 888);

    my $i = 0;
    foreach my $tag (@tags) {
      my $expected = $plex_expected_read_counts->[$i];
      my $count    = $pub->num_reads($plex_position, tag_index => $tag);

      cmp_ok($count, '==', $expected,
             "num_reads for $file_format position $plex_position tag $tag") or
               diag explain $count;
      $i++;
    }
  }

  # Plex-level alignment filter
  my $plex_phix_expected_read_counts = [32924, # tag 0
                                        # tag 1 has no JSON file
                                        94,   8, 22, 52,
                                        # tag 8 has no JSON file
                                        28, 110, 50, 20,
                                        22,  20, 24, 18,
                                        24,  12, 24, 18,
                                       ]; # tag 888 has no JSON file

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $plex_runfolder_path);

    my @tags = (0 .. $tag_count);

    my $i = 0;
    foreach my $tag (@tags) {
      my $expected = $plex_phix_expected_read_counts->[$i];
      my $count    = $pub->num_reads($plex_position,
                                     alignment_filter => 'phix',
                                     tag_index        => $tag);

      cmp_ok($count, '==', $expected,
             "num_reads for $file_format position $plex_position " .
             "tag $tag phix")
        or diag explain $count;
      $i++;
    }
  }
}

sub is_paired_read : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    ok($pub->is_paired_read, "$runfolder_path is paired read");
  }
}

sub list_xml_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my @expected_files = ("$runfolder_path/RunInfo.xml",
                          "$runfolder_path/runParameters.xml");
    my $observed_files = $pub->list_xml_files;
    is_deeply($observed_files, \@expected_files,
              "Found XML files ($file_format)")
        or diag explain $observed_files;
  }
}

sub list_lane_alignment_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_lane_alignment_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_lane_alignment_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found lane $position alignment files ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_alignment_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index =
      calc_plex_alignment_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_plex_alignment_files($position);
       is_deeply($observed_files, \@expected_files,
                 "Found plex alignment files for lane $position " .
                 "($file_format)")
         or diag explain $observed_files;
    }
  }
}

sub list_lane_index_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_lane_index_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my $observed_files = $pub->list_lane_index_files($position);
      my @expected_files = @{$position_index{$position}};
      is_deeply($observed_files, \@expected_files,
                "Found lane $position index files ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_index_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index =
      calc_plex_index_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_plex_index_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex index files for lane $position " .
                "($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_lane_qc_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_lane_qc_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_lane_qc_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found lane $position QC files ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_qc_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_plex_qc_files($archive_path);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_plex_qc_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex QC files for lane $position ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_lane_ancillary_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_lane_ancillary_files($archive_path, $file_format);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_lane_ancillary_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found lane $position ancillary files ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_ancillary_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  foreach my $file_format (qw[bam cram]) {
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);

    my %position_index = calc_plex_ancillary_files($archive_path);

    foreach my $position (1 .. 8) {
      my @expected_files = @{$position_index{$position}};
      my $observed_files = $pub->list_plex_ancillary_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex ancillary files for lane $position " .
                "($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub publish_xml_files : Test(15) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $dest_coll      = "$irods_tmp_coll/publish_xml_files";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => 'cram',
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my @expected_paths = ("$dest_coll/RunInfo.xml",
                        "$dest_coll/runParameters.xml");
  my ($num_files, $num_processed, $num_errors) = $pub->publish_xml_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 2, 'Published 2 XML files');

  my @observed_paths = observed_data_objects($irods, $dest_coll, '[.]xml$');
  is_deeply(\@observed_paths, \@expected_paths,
            "Published correctly named XML files") or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

  foreach my $path (@observed_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::XMLDataObject->new($irods, $path);
    cmp_ok($obj->get_avu($ID_RUN)->{value}, '==', 18448,
           "$path id_run metadata present");
  }
}

sub publish_interop_files : Test(45) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $dest_coll      = "$irods_tmp_coll/publish_interop_files";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => 'cram',
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

   my @expected_paths = map { "$dest_coll/InterOp/$_" }
     qw[ControlMetricsOut.bin
        CorrectedIntMetricsOut.bin
        ErrorMetricsOut.bin
        ExtractionMetricsOut.bin
        ImageMetricsOut.bin
        QMetricsOut.bin
        TileMetricsOut.bin];
  my ($num_files, $num_processed, $num_errors) = $pub->publish_interop_files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', 7, 'Published 7 InterOp files');

  my @observed_paths = observed_data_objects($irods,
                                             $pub->interop_dest_collection,
                                             '[.]bin$');
  is_deeply(\@observed_paths, \@expected_paths,
            "Published correctly named InterOp files") or
              diag explain \@observed_paths;

  check_common_metadata($irods, @observed_paths);

  foreach my $path (@observed_paths) {
    my $obj = WTSI::NPG::HTS::Illumina::InterOpDataObject->new($irods, $path);
    cmp_ok($obj->get_avu($ID_RUN)->{value}, '==', 18448,
           "$path id_run metadata present");
  }
}

sub publish_lane_alignment_files_mlwh : Test(272) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  check_publish_lane_alignment_files($runfolder_path, $archive_path,
                                     $lims_factory);
}

sub publish_lane_alignment_files_samplesheet : Test(272) {
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$runfolder_path/Data/Intensities/BAM_basecalls_20151214-085833/metadata_cache_18448/samplesheet_18448.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  check_publish_lane_alignment_files($runfolder_path, $archive_path,
                                     $lims_factory);
}

sub publish_plex_alignment_files_mlwh : Test(813) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  check_publish_plex_alignment_files($runfolder_path, $archive_path,
                                     $lims_factory);
}

sub publish_plex_alignment_files_samplesheet : Test(813) {
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/metadata_cache_17550/samplesheet_17550.csv";

  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  check_publish_plex_alignment_files($runfolder_path, $archive_path,
                                     $lims_factory);
}

sub publish_lane_index_files : Test(99) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';
  my $dest_coll = "$irods_tmp_coll/publish_lane_index_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => $file_format,
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my %position_index = calc_lane_index_files($archive_path, $file_format);

  foreach my $position (1 .. 8) {
    my $num_expected  = scalar @{$position_index{$position}};

    # Lane 1 has been marked as having 0 reads
    if ($position == 1) {
      $num_expected--;
    }

    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_lane_index_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format lane $position index files");

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;

    # Lane 1 has been marked as having 0 reads
    my @expected_paths =
      grep { $_ !~ m{18448_1.cram.crai$} }
      expected_data_objects($dest_coll, \%position_index, $position);

    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format index files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
  }
}

sub publish_plex_index_files : Test(271) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/publish_plex_index_files/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index =
      calc_plex_index_files($archive_path, $file_format);

    my $num_expected  = scalar @{$position_index{$position}};
    # Lane 1, plex 1 has been marked as having 0 reads
    if ($position == 1) {
      $num_expected--;
    }

    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_index_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected position $position " .
           "$file_format index files");

    my $pos_pattern = sprintf '%d_%d#\d+', $pub->id_run, $position;

    # Lane 1, plex 1 has been marked as having 0 reads
    my @expected_paths =
      grep { $_ !~ m{17550_1\#1[.]cram[.]crai$} }
      expected_data_objects($dest_coll, \%position_index, $position);

    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format index files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
  }
}

sub publish_lane_ancillary_files : Test(864) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';
  my $dest_coll = "$irods_tmp_coll/publish_lane_ancillary_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => $file_format,
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my %position_index = calc_lane_ancillary_files($archive_path, $file_format);

  foreach my $position (1 .. 8) {
    my $num_expected  = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_lane_ancillary_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format lane " .
           "$position $file_format ancillary files");

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named $file_format position $position " .
              "ancillary files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
  }
}

sub publish_plex_ancillary_files : Test(2806) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/publish_plex_ancillary_files/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index = calc_plex_ancillary_files($archive_path);

    my $num_expected  = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_ancillary_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format position $position " .
           "ancillary files");

    my $pos_pattern = sprintf '%d_%d#\d+', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named $file_format position $position " .
              "ancillary files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
  }
}

sub publish_lane_qc_files : Test(744) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/151211_HX3_18448_B_HHH55CCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20151214-085833/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';
  my $dest_coll = "$irods_tmp_coll/publish_lane_qc_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => $file_format,
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my %position_index = calc_lane_qc_files($archive_path, $file_format);

  foreach my $position (1 .. 8) {
    my $num_expected  = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_lane_qc_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format lane " .
           "$position QC files");

    my $qc_coll = catfile($dest_coll, q[qc]);

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($qc_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $qc_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named $file_format position $position " .
              "JSON QC files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
    check_study_id_metadata($irods, @observed_paths);
  }
}

sub publish_plex_qc_files : Test(1662) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/publish_plex_qc_files/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index = calc_plex_qc_files($archive_path);

    my $num_expected  = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_qc_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format position $position " .
           "JSON QC files");

    my $qc_coll = catfile($dest_coll, q[qc]);

    my $pos_pattern = sprintf '%d_%d#\d+', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($qc_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $qc_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named $file_format position $position " .
              "JSON QC files") or
                diag explain \@observed_paths;

    check_common_metadata($irods, @observed_paths);
    check_study_id_metadata($irods, @observed_paths);
  }
}

sub publish_plex_alignment_files_alt_process : Test(924) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';
  my $alt_process = 'an_alternative_process';

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/alt_process/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (alt_process     => $alt_process,
       dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index =
      calc_plex_alignment_files($archive_path, $file_format);

    my $num_expected  = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_alignment_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected position $position " .
           "$file_format alignment files");

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format alt process alignment files") or
                diag explain \@observed_paths;

    check_primary_metadata($irods, @observed_paths);
    check_common_metadata($irods, @observed_paths);
    check_study_metadata($irods, @observed_paths);
    check_alt_process_metadata($irods, $alt_process, @observed_paths);
  }
}

sub publish_plex_alignment_files_human_split : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/160225_HS38_18980_B_H7HHMBCXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20160228-062603/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);

  my $file_format = 'cram';
  my $position = 1;
  my $dest_coll = "$irods_tmp_coll/plex_human_split/$position";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => $file_format,
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my $num_expected = 5;

  my @listed = @{$pub->list_plex_alignment_files($position)};
  my $num_listed = scalar @listed;
  cmp_ok($num_listed, '==', $num_expected,
         "$num_listed position $position alignment files to publish") or
           diag explain \@listed;

  my ($num_files, $num_processed, $num_errors) =
    $pub->publish_plex_alignment_files($position);

  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_processed, '==', $num_expected,
         "Published $num_expected position $position " .
         "$file_format alignment files");
}

sub publish_with_samplesheet_driver : Test(762) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $archive_path   = "$runfolder_path/Data/Intensities/" .
                       'BAM_basecalls_20150914-100512/no_cal/archive';
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/metadata_cache_17550/samplesheet_17550.csv";

  my $file_format = 'cram';

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/publish_with_samplesheet_driver/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index =
      calc_plex_alignment_files($archive_path, $file_format);

    my $num_expected = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_alignment_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected position $position " .
           "$file_format alignment files");

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format alignment files") or
                diag explain \@observed_paths;

    check_primary_metadata($irods, @observed_paths);
    check_common_metadata($irods, @observed_paths);
    check_study_metadata($irods, @observed_paths);
  }
}

sub dest_collection : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/100818_IL32_05174";
  my $lims_factory =
    WTSI::NPG::HTS::LIMSFactory->new(driver_type => 'samplesheet');

  foreach my $file_format (qw[bam cram]) {
    my $pub1 = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);
    is($pub1->dest_collection, '/seq/5174', 'Default dest collection');

    my $pub2 = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => '/a/b/c',
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       runfolder_path  => $runfolder_path);
    is($pub2->dest_collection, '/a/b/c', 'Custom dest collection');

    # Alt process 
    my $pub3 = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (alt_process    => 'x',
       file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);
    is($pub3->dest_collection, '/seq/5174/x',
       'Default alt_process destination has process appended to collection');

    my $pub4 = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (alt_process    => 'x',
       dest_collection => '/a/b/c',
       file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       runfolder_path => $runfolder_path);
    is($pub4->dest_collection, '/a/b/c',
       'Custom alt_process destination uses the provided collection');
  }
}

# From here onwards are test support methods

sub check_publish_lane_alignment_files {
  my ($runfolder_path, $archive_path, $lims_factory) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $file_format = 'cram';
  my $dest_coll = "$irods_tmp_coll/publish_lane_alignment_files";

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
    (dest_collection => $dest_coll,
     file_format     => $file_format,
     irods           => $irods,
     lims_factory    => $lims_factory,
     restart_file    => catfile($tmpdir->dirname, 'published.json'),
     runfolder_path  => $runfolder_path);

  my %position_index = calc_lane_alignment_files($archive_path, $file_format);

  foreach my $position (1 .. 8) {
    my @expected_files = @{$position_index{$position}};
    my $num_expected  = scalar @expected_files;

    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_lane_alignment_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected $file_format lane " .
           "$position alignment files");

    my $pos_pattern = sprintf '%d_%d.*\.%s$',
      $pub->id_run, $position, $file_format;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format alignment files") or
                diag explain \@observed_paths;

    check_primary_metadata($irods, @observed_paths);
    check_common_metadata($irods, @observed_paths);
    check_study_metadata($irods, @observed_paths);
  }

  $pub->write_restart_file;

  # FIXME -- these tests could be more exhaustive
  my $expected_read_counts = [0,         # edited from 863061182,
                              856966676,
                              898136862,
                              893691470,
                              869960390,
                              894014820,
                              883399526,
                              899795972];

  foreach my $position (1 .. 8) {
    my $expected_read_count = $expected_read_counts->[$position - 1];

    my @paths = $irods->find_objects_by_meta($irods_tmp_coll,
                                             [lane   => $position],
                                             [type   => 'cram'],
                                             [target => 1]);
    cmp_ok(scalar @paths, '==', 1, "position: $position found");

    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new($irods, $paths[0]);
    is_deeply([$obj->get_avu($IS_PAIRED_READ)],
              [{attribute => $IS_PAIRED_READ,
                value     => 1}],
              "lane $position paired_read metadata correct");

    is_deeply([$obj->find_in_metadata($TOTAL_READS)],
              [{attribute => $TOTAL_READS,
                value     => $expected_read_count}],
              "lane $position total_reads metadata correct");
  }
}

sub check_publish_plex_alignment_files {
  my ($runfolder_path, $archive_path, $lims_factory) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $file_format = 'cram';
  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    my $dest_coll = "$irods_tmp_coll/publish_plex_alignment_files/$position";

    my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
    my $pub = WTSI::NPG::HTS::Illumina::RunPublisher->new
      (dest_collection => $dest_coll,
       file_format     => $file_format,
       irods           => $irods,
       lims_factory    => $lims_factory,
       restart_file    => catfile($tmpdir->dirname, 'published.json'),
       runfolder_path  => $runfolder_path);

    my %position_index =
      calc_plex_alignment_files($archive_path, $file_format);

    my $num_expected = scalar @{$position_index{$position}};
    my ($num_files, $num_processed, $num_errors) =
      $pub->publish_plex_alignment_files($position);

    cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
    cmp_ok($num_processed, '==', $num_expected,
           "Published $num_expected position $position " .
           "$file_format alignment files");

    my $pos_pattern = sprintf '%d_%d', $pub->id_run, $position;
    my @expected_paths =
      expected_data_objects($dest_coll, \%position_index, $position);
    my @observed_paths =
      observed_data_objects($irods, $dest_coll, $pos_pattern);

    is_deeply(\@observed_paths, \@expected_paths,
              "Published correctly named position $position " .
              "$file_format alignment files") or
                diag explain \@observed_paths;

    check_primary_metadata($irods, @observed_paths);
    check_common_metadata($irods, @observed_paths);
    check_study_metadata($irods, @observed_paths);
  }

  # FIXME -- these tests could be more exhaustive
  my $tag_count = 16;
  my $expected_read_counts = [3334934,  # tag 0
                              0,        # edited from 71488156,
                              29817458, 15354480, 33948370,
                              33430552, 24094786, 32604688, 26749430,
                              27668866, 30775624, 33480806, 40965140,
                              32087634, 37315470, 27193418, 31538878,
                              1757876]; # tag 888
  # Other tags
  my @tags = (1 .. $tag_count, 888);

  my $i = 1;
  foreach my $tag (@tags) {
    my $expected_read_count = $expected_read_counts->[$i];

    my @paths = $irods->find_objects_by_meta($irods_tmp_coll,
                                             [lane      => 1],
                                             [tag_index => $tag],
                                             [type      => 'cram'],
                                             [target    => 1]);

    cmp_ok(scalar @paths, '==', 1, "position: 1, tag $tag found");

    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new($irods, $paths[0]);
    is_deeply([$obj->get_avu($IS_PAIRED_READ)],
              [{attribute => $IS_PAIRED_READ,
                value     => 1}],
              "tag $tag paired_read metadata correct");

    is_deeply([$obj->find_in_metadata($TOTAL_READS)],
              [{attribute => $TOTAL_READS,
                value     => $expected_read_count}],
              "tag $tag total_reads metadata correct");
    $i++;
  }
}

sub calc_lane_alignment_files {
  my ($root_path, $file_format) = @_;

  my %position_index;

  my $id_run = 18448;

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

sub calc_plex_alignment_files {
  my ($root_path, $file_format) = @_;

  my %position_index;

  my $id_run = 17550;
  my $lane_tag_counts = {1 => 16,
                         2 => 12,
                         3 =>  8,
                         4 =>  8,
                         5 =>  5,
                         6 => 12,
                         7 =>  6,
                         8 =>  6};
  my $lane_yhuman = 6;

  foreach my $position (sort keys %{$lane_tag_counts}) {
    # All lanes have tag 888
    my @tags = (0 .. $lane_tag_counts->{$position}, 888);

    my @plex_files;
    foreach my $tag (@tags) {
      push @plex_files, sprintf '%s/lane%d/%d_%d#%d.%s',
        $root_path, $position, $id_run, $position, $tag, $file_format;

      if ($tag != 888) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d_phix.%s',
          $root_path, $position, $id_run, $position, $tag, $file_format;
      }

      if ($position == $lane_yhuman and $tag != 888) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d_yhuman.%s',
          $root_path, $position, $id_run, $position, $tag, $file_format;
      }
    }

    @plex_files = sort @plex_files;
    $position_index{$position} = \@plex_files;
  }

  return %position_index;
}

sub calc_lane_index_files {
  my ($root_path, $file_format) = @_;

  my %position_index;

  my $id_run = 18448;
  my $index_suffix;
  if ($file_format eq 'bam') {
    $index_suffix = 'bai';
  }
  elsif ($file_format eq 'cram') {
    $index_suffix = 'cram.crai';
  }
  else {
    fail "Unknown file format '$file_format'";
  }

  foreach my $position (1 .. 8) {
    my @lane_files;
    push @lane_files,  sprintf '%s/%d_%d.%s',
      $root_path, $id_run, $position, $index_suffix;
    push @lane_files,  sprintf '%s/%d_%d_phix.%s',
      $root_path, $id_run, $position, $index_suffix;

    @lane_files = sort @lane_files;
    $position_index{$position} = \@lane_files;
  }

  return %position_index;
}

sub calc_plex_index_files {
  my ($root_path, $file_format) = @_;

  my %position_index;

  my $id_run = 17550;
  my $lane_tag_counts = {1 => 16,
                         2 => 12,
                         3 =>  8,
                         4 =>  8,
                         5 =>  5,
                         6 => 12,
                         7 =>  6,
                         8 =>  6};
  my $lane_yhuman = 6;
  my $index_suffix;
  if ($file_format eq 'bam') {
    $index_suffix = 'bai';
  }
  elsif ($file_format eq 'cram') {
    $index_suffix = 'cram.crai';
  }
  else {
    fail "Unknown file format '$file_format'";
  }

  foreach my $position (sort keys %{$lane_tag_counts}) {
    # All lanes have tag 888
    my @tags = (0 .. $lane_tag_counts->{$position}, 888);

    my @plex_files;
    foreach my $tag (@tags) {
      if ($position != 5 or
          ($position == 5 and $tag != 5)) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d.%s',
          $root_path, $position, $id_run, $position, $tag, $index_suffix;
      }

      if ($tag != 888) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d_phix.%s',
          $root_path, $position, $id_run, $position, $tag, $index_suffix;
      }

      if ($position == $lane_yhuman and $tag != 888) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d_yhuman.%s',
          $root_path, $position, $id_run, $position, $tag, $index_suffix;
      }
    }

    @plex_files = sort @plex_files;
    $position_index{$position} = \@plex_files;
  }

  return %position_index;
}

sub calc_lane_qc_files {
  my ($root_path) = @_;

  my %position_index;

  my $id_run = 18448;
  my @qc_metrics = qw[adapter alignment_filter_metrics bam_flagstats
                      gc_bias gc_fraction genotype insert_size qX_yield
                      ref_match sequence_error sequence_summary spatial_filter
                      verify_bam_id];

  # We no longer load these because they are a QC implementation detail
  # my @qc_parts = qw[_F0x900.samtools_stats _F0xB00.samtools_stats];

  foreach my $position (1 .. 8) {
    my @lane_files;
    foreach my $metric (@qc_metrics) {
      push @lane_files, sprintf '%s/qc/%d_%d.%s.json',
        $root_path, $id_run, $position, $metric;

      if ($metric eq 'bam_flagstats' or
          $metric eq 'sequence_summary') {
        push @lane_files, sprintf '%s/qc/%d_%d_phix.%s.json',
          $root_path, $id_run, $position, $metric;
      }
    }

    # foreach my $part (@qc_parts) {
    #   push @lane_files, sprintf '%s/qc/%d_%d%s.json',
    #     $root_path, $id_run, $position, $part;
    #   push @lane_files, sprintf '%s/qc/%d_%d_phix%s.json',
    #     $root_path, $id_run, $position, $part;
    # }

    @lane_files = sort @lane_files;
    $position_index{$position} = \@lane_files;
  }

  return %position_index;
}

sub calc_plex_qc_files {
  my ($root_path) = @_;

  my %position_index;

  my $id_run = 17550;
  my $lane_tag_counts = {1 => 16,
                         2 => 12,
                         3 =>  8,
                         4 =>  8,
                         5 =>  5,
                         6 => 12,
                         7 =>  6,
                         8 =>  6};
  my $lane_yhuman = 6;

  my @qc_metrics = qw[adapter bam_flagstats gc_bias gc_fraction insert_size
                      qX_yield ref_match sequence_error];

  # This enumerates all the edge cases I found in this example
  # dataset. Rather than simply listing all the expected files in each
  # case, it allows us to see the scope for normalising the outputs in
  # future. It makes maintaining the tests easier too.
  foreach my $position (sort keys %{$lane_tag_counts}) {
    # All lanes have tag 888
    my @tags = (0 .. $lane_tag_counts->{$position}, 888);

    my @plex_files;
    foreach my $tag (@tags) {
      my @metrics = @qc_metrics;
      if (not ($tag       == 0                  or
               $position   < 5                  or
               ($position == 5 and $tag > 4)    or
               ($position == 5 and $tag == 888) or
               ($position == 6 and $tag == 888) or
               $position > 6)) {
        push @metrics, 'genotype';
      }

      if ($tag != 888) {
        # Only for non-phiX tags
        push @metrics, 'alignment_filter_metrics';
      }

      foreach my $metric (@metrics) {
        if ($metric eq 'bam_flagstats') {
          # In some cases flagstats is named inconsistently (underscore)
          if ($position == 5 and $tag == 5) {
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_phix_%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
          }
          elsif ($tag == 888) {
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
          }
          else {
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d.%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_phix.%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
          }

          # Lane 6 has a yhuman split
          if ($position == 6 and $tag != 888) {
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_yhuman.%s.json',
              $root_path, $position, $id_run, $position, $tag, $metric;
          }
        }
        else {
          push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d.%s.json',
            $root_path, $position, $id_run, $position, $tag, $metric;
        }
      }
    }

    @plex_files = sort @plex_files;
    $position_index{$position} = \@plex_files;
  }

  return %position_index;
}

sub calc_lane_ancillary_files {
  my ($root_path) = @_;

  my %position_index;

  my $id_run = 18448;
  my @default_parts = qw[.bam_stats
                         .bamcheck
                         .flagstat
                         .seqchksum
                         .sha512primesums512.seqchksum
                         _quality_cycle_caltable.txt
                         _quality_cycle_surv.txt
                         _quality_error.txt
                         _F0x900.stats
                         _F0xB00.stats];

  foreach my $position (1 .. 8) {
    my @lane_files;
    push @lane_files, sprintf '%s/%d_%d%s',
      $root_path, $id_run, $position, '.all.seqchksum';

    foreach my $part (@default_parts) {
      push @lane_files, sprintf '%s/%d_%d%s',
        $root_path, $id_run, $position, $part;
      push @lane_files, sprintf '%s/%d_%d_phix%s',
        $root_path, $id_run, $position, $part;
    }

    @lane_files = sort @lane_files;
    $position_index{$position} = \@lane_files;
  }

  return %position_index;
}

sub calc_plex_ancillary_files {
  my ($root_path) = @_;

  my %position_index;

  my $id_run = 17550;
  my $lane_tag_counts = {1 => 16,
                         2 => 12,
                         3 =>  8,
                         4 =>  8,
                         5 =>  5,
                         6 => 12,
                         7 =>  6,
                         8 =>  6};
  my $lane_nuc_type = {1 => 'DNA',
                       2 => 'DNA',
                       3 => 'RNA',
                       4 => 'RNA',
                       5 => 'DNA',
                       6 => 'DNA',
                       7 => 'DNA',
                       8 => 'DNA'};
  my $lane_yhuman = 6;

  my @default_parts = qw[.bam_stats
                         .bamcheck
                         .flagstat
                         .seqchksum
                         .sha512primesums512.seqchksum];

  foreach my $position (sort keys %{$lane_tag_counts}) {
    # All lanes have tag 888
    my @tags = (0 .. $lane_tag_counts->{$position}, 888);

    my @plex_files;
    foreach my $tag (@tags) {
      foreach my $part (@default_parts) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d%s',
          $root_path, $position, $id_run, $position, $tag, $part;

        if ($tag != 888) {
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d_phix%s',
            $root_path, $position, $id_run, $position, $tag, $part;
        }

        if ($position == $lane_yhuman and $tag != 888) {
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d_yhuman%s',
            $root_path, $position, $id_run, $position, $tag, $part;
        }
      }

      foreach my $part (qw[_quality_cycle_caltable.txt
                           _quality_cycle_surv.txt
                           _quality_error.txt]) {
        push @plex_files, sprintf '%s/lane%d/%d_%d#%d%s',
          $root_path, $position, $id_run, $position, $tag, $part;

        if ($tag != 888) {
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d_phix%s',
            $root_path, $position, $id_run, $position, $tag, $part;
        }
      }

      foreach my $part (qw[.deletions.bed
                           .insertions.bed
                           .junctions.bed]) {
        if ($lane_nuc_type->{$position} eq 'RNA' and
            $tag != 0                            and
            $tag != 888) {
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d%s',
            $root_path, $position, $id_run, $position, $tag, $part;
        }
      }

      foreach my $part (qw[_F0x900.stats _F0xB00.stats]) {
        if ($tag != 888) {
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d%s',
            $root_path, $position, $id_run, $position, $tag, $part;
          push @plex_files, sprintf '%s/lane%d/%d_%d#%d_phix%s',
            $root_path, $position, $id_run, $position, $tag, $part;

          if ($position == $lane_yhuman) {
            push @plex_files, sprintf '%s/lane%d/%d_%d#%d_yhuman%s',
              $root_path, $position, $id_run, $position, $tag, $part;
          }
        }
      }
    }

    # These files are missing from the example dataset (because they
    # are missing in production)
    if ($position == 5) {
      my %missing = map { $_ => 1 }
        map { sprintf '%s/lane%d/17550_%d#5%s',
              $root_path, $position, $position, $_ }
        qw[_F0x900.stats
           _F0xB00.stats
           _quality_cycle_surv.txt
           _quality_cycle_caltable.txt
           _quality_error.txt
           _phix_F0x900.stats
           _phix_F0xB00.stats];
      @plex_files = grep { not $missing{$_} } @plex_files;
    }

    @plex_files = sort @plex_files;
    $position_index{$position} = \@plex_files;
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
  my ($irods, $dest_collection, $regex) = @_;

  my ($observed_paths) = $irods->list_collection($dest_collection);
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

    foreach my $attr ($ALIGNMENT, $ID_RUN, $POSITION,
                      $TOTAL_READS, $IS_PAIRED_READ,
                      $WTSI::NPG::HTS::Illumina::Annotator::SEQCHKSUM) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
}

sub check_study_id_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::Illumina::AncDataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my @avu = $obj->find_in_metadata($STUDY_ID);
    cmp_ok(scalar @avu, '>=', 1, "$file_name $STUDY_ID metadata present");
  }
}

sub check_study_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::Illumina::AncDataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my $tag_index = $obj->tag_index;

    # Tag 888 has no study accession or name
    if ($tag_index and $tag_index == 888) {
      foreach my $attr ($STUDY_ID, $STUDY_NAME) {
        my @avu = $obj->find_in_metadata($attr);
        # FIXME -- Should this be >= 1 or == 1 ?
        cmp_ok(scalar @avu, '>=', 1, "$file_name $attr metadata present");
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
  my ($irods, $alt_process, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new($irods, $path);
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

1;
