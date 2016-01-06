package WTSI::NPG::HTS::RunPublisherTest;

use strict;
use warnings;

use Carp;
use English qw(-no_match_vars);
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw(WTSI::NPG::HTS::Test);

use WTSI::NPG::HTS::RunPublisher;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $test_counter = 0;
my $data_path = 't/data/run_publisher';
my $fixture_path = "t/fixtures";

my $db_dir = File::Temp->newdir;
my $qc_schema;
my $wh_schema;
my $lims_factory;

my $irods_tmp_coll;

my $pid = $PID;

sub setup_databases : Test(startup) {
  my $qc_db_file = catfile($db_dir, 'npg_qc.db');
  my $qc_attr = {RaiseError => 1};
  {
    # create_test_db produces warnings during expected use, which
    # appear mixed with test output in the terminal
    local $SIG{__WARN__} = sub { };
    $qc_schema = TestDB->new(test_dbattr => $qc_attr)->create_test_db
      ('npg_qc::Schema', "$fixture_path/npgqc", $qc_db_file);
  }

  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  my $wh_attr = {RaiseError    => 1,
                 on_connect_do => 'PRAGMA encoding = "UTF-8"'};

  {
    local $SIG{__WARN__} = sub { };
    $wh_schema = TestDB->new(test_dbattr => $wh_attr)->create_test_db
      ('WTSI::DNAP::Warehouse::Schema', "$fixture_path/ml_warehouse",
       $wh_db_file);
  }

  $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
}

sub teardown_databases : Test(shutdown) {
  $qc_schema->storage->disconnect;
  $wh_schema->storage->disconnect;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("HTSRunPublisherTest.$pid.$test_counter");
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

  foreach my $file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format  => $file_format,
       irods        => $irods,
       lims_factory => $lims_factory,
       npgqc_schema => $qc_schema,
       runfolder_path => $runfolder_path);

    is_deeply([$pub->positions], [1 .. 8],
              "Found expected positions ($file_format)")
      or diag explain $pub->positions;
  }
}

sub num_total_reads : Test(36) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";
  my $position  = 1;
  my $tag_count = 16,
  my $expected_read_counts = [3334934,  # tag 0
                              71488156, 29817458, 15354480, 33948370,
                              33430552, 24094786, 32604688, 26749430,
                              27668866, 30775624, 33480806, 40965140,
                              32087634, 37315470, 27193418, 31538878,
                              1757876]; # tag 888

  foreach my $file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

    my @tags = (0 .. $tag_count, 888);

    my $i = 0;
    foreach my $tag (@tags) {
      my $expected = $expected_read_counts->[$i];
      my $count = $pub->num_total_reads($position, $tag);

      cmp_ok($count, '==', $expected,
             "num_total_reads for position $position tag $tag") or
               diag explain $count;
      $i++;
    }
  }
}

sub is_paired_read : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  foreach my $file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

    ok($pub->is_paired_read, "$runfolder_path is paired read");
  }
}

sub list_plex_alignment_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $id_run = 17550;
  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  foreach my $file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

    my $lane_tag_counts = {1 => 16,
                           2 => 12,
                           3 =>  8,
                           4 =>  8,
                           5 =>  5,
                           6 => 12,
                           7 =>  6,
                           8 =>  6};
    my $lane_yhuman = 6;
    my $archive_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive";

    foreach my $position (sort keys %{$lane_tag_counts}) {
      # All lanes have tag 888
      my @tags = (0 .. $lane_tag_counts->{$position}, 888);

      my @plex_files;
      foreach my $tag (@tags) {
        push @plex_files, sprintf "%s/lane%d/%d_%d#%d.%s",
          $archive_path, $position, $id_run, $position, $tag, $file_format;

        if ($tag != 888) {
          push @plex_files, sprintf "%s/lane%d/%d_%d#%d_phix.%s",
            $archive_path, $position, $id_run, $position, $tag, $file_format;
        }

        if ($position == $lane_yhuman and $tag != 888) {
          push @plex_files, sprintf "%s/lane%d/%d_%d#%d_yhuman.%s",
            $archive_path, $position, $id_run, $position, $tag, $file_format;
        }
      }

      my @expected_files = sort @plex_files;
      my $observed_files = $pub->list_plex_alignment_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex alignment files for lane $position ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_qc_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  foreach my $file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

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
    my $archive_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive";

    my @qc_metrics = qw(adapter bam_flagstats gc_bias gc_fraction insert_size
                        qX_yield ref_match sequence_error);

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
                $archive_path, $position, $id_run, $position, $tag, $metric;
              push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_phix_%s.json',
                $archive_path, $position, $id_run, $position, $tag, $metric;
            }
            elsif ($tag == 888) {
              push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_%s.json',
                $archive_path, $position, $id_run, $position, $tag, $metric;
            }
            else {
              push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d.%s.json',
                $archive_path, $position, $id_run, $position, $tag, $metric;
              push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_phix.%s.json',
                $archive_path, $position, $id_run, $position, $tag, $metric;
            }

            # Lane 6 has a yhuman split
            if ($position == 6 and $tag != 888) {
              push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d_yhuman.%s.json',
                $archive_path, $position, $id_run, $position, $tag, $metric;
            }
          }
          else {
            push @plex_files, sprintf '%s/lane%d/qc/%d_%d#%d.%s.json',
              $archive_path, $position, $id_run, $position, $tag, $metric;
          }
        }
      }

      my @expected_files = sort @plex_files;
      my $observed_files = $pub->list_plex_qc_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex QC files for lane $position ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub list_plex_ancillary_files : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  foreach my$file_format (qw(bam cram)) {
    my $pub = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

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
    my $archive_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive";

    my @default_parts = qw(.bamcheck
                           .flagstat
                           .seqchksum
                           .sha512primesums512.seqchksum);

    foreach my $position (sort keys %{$lane_tag_counts}) {
      # All lanes have tag 888
      my @tags = (0 .. $lane_tag_counts->{$position}, 888);

      my @plex_files;
      foreach my $tag (@tags) {
        foreach my $part (@default_parts) {
          push @plex_files, sprintf "%s/lane%d/%d_%d#%d%s",
            $archive_path, $position, $id_run, $position, $tag, $part;

          if ($tag != 888) {
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d_phix%s",
              $archive_path, $position, $id_run, $position, $tag, $part;
          }

          if ($position == $lane_yhuman and $tag != 888) {
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d_yhuman%s",
              $archive_path, $position, $id_run, $position, $tag, $part;
          }
        }

        foreach my $part (qw(_quality_cycle_caltable.txt
                             _quality_cycle_surv.txt
                             _quality_error.txt)) {
          push @plex_files, sprintf "%s/lane%d/%d_%d#%d%s",
            $archive_path, $position, $id_run, $position, $tag, $part;

          if ($tag != 888) {
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d_phix%s",
              $archive_path, $position, $id_run, $position, $tag, $part;
          }
        }

        foreach my $part (qw(.deletions.bed
                             .insertions.bed
                             .junctions.bed)) {
          if ($lane_nuc_type->{$position} eq 'RNA' and
              $tag != 0                            and
              $tag != 888) {
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d%s",
              $archive_path, $position, $id_run, $position, $tag, $part;
          }
        }

        foreach my $part (qw(_F0x900.stats _F0xB00.stats)) {
          if ($tag != 888) {
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d%s",
              $archive_path, $position, $id_run, $position, $tag, $part;
            push @plex_files, sprintf "%s/lane%d/%d_%d#%d_phix%s",
              $archive_path, $position, $id_run, $position, $tag, $part;

            if ($position == $lane_yhuman) {
              push @plex_files, sprintf "%s/lane%d/%d_%d#%d_yhuman%s",
                $archive_path, $position, $id_run, $position, $tag, $part;
            }
          }
        }
      }

      # These files are missing from the example dataset (because they
      # are missing in production)
      if ($position == 5) {
        my %missing = map { $_ => 1 }
                      map { sprintf '%s/lane%d/17550_%d#5%s',
                            $archive_path, $position, $position, $_ }
                      qw(_F0x900.stats
                         _F0xB00.stats
                         _quality_cycle_surv.txt
                         _quality_cycle_caltable.txt
                         _quality_error.txt
                         _phix_F0x900.stats
                         _phix_F0xB00.stats);
        @plex_files = grep { not $missing{$_} } @plex_files;
      }

      my @expected_files = sort @plex_files;
      my $observed_files = $pub->list_plex_ancillary_files($position);
      is_deeply($observed_files, \@expected_files,
                "Found plex ancillary files for lane $position ($file_format)")
        or diag explain $observed_files;
    }
  }
}

sub publish_plex_alignment_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    # Omitting bam
    foreach my $file_format (qw(cram)) {
      my $pub = WTSI::NPG::HTS::RunPublisher->new
        (collection     => "$irods_tmp_coll/publish_alignment_files",
         file_format    => $file_format,
         irods          => $irods,
         lims_factory   => $lims_factory,
         npgqc_schema   => $qc_schema,
         runfolder_path => $runfolder_path);

      ok($pub->publish_plex_alignment_files($position),
         "Published position $position $file_format alignment files");
    }
  }
}

sub publish_plex_ancillary_files : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/150910_HS40_17550_A_C75BCANXX";

  # Position 1 is DNA, position 3 is RNA
  foreach my $position (1, 3) {
    # Omitting bam
    foreach my $file_format (qw(cram)) {
      my $pub = WTSI::NPG::HTS::RunPublisher->new
        (collection     => "$irods_tmp_coll/publish_ancillary_files",
         file_format    => $file_format,
         irods          => $irods,
         lims_factory   => $lims_factory,
         npgqc_schema   => $qc_schema,
         runfolder_path => $runfolder_path);

      ok($pub->publish_plex_ancillary_files($position),
         "Published position $position $file_format ancillary files");
    }
  }
}

sub collection : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $runfolder_path = "$data_path/sequence/100818_IL32_05174";

  foreach my $file_format (qw(bam cram)) {
    my $pub1 = WTSI::NPG::HTS::RunPublisher->new
      (file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

    is($pub1->collection, '/seq/5174', 'Default collection');

    my $pub2 = WTSI::NPG::HTS::RunPublisher->new
      (collection     => '/a/b/c',
       file_format    => $file_format,
       irods          => $irods,
       lims_factory   => $lims_factory,
       npgqc_schema   => $qc_schema,
       runfolder_path => $runfolder_path);

    is($pub2->collection, '/a/b/c', 'Custom collection');
  }
}

1;
