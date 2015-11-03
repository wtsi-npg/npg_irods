package WTSI::NPG::HTS::HTSFileDataObjectTest;

use strict;
use warnings;

use Carp;
use File::Spec;
use File::Temp;
use List::AllUtils qw(each_array);
use Log::Log4perl;
use Test::More tests => 124;

use base qw(Test::Class);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::HTS::HTSFileDataObject') }

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::HTSFileDataObject;
use WTSI::NPG::HTS::Samtools;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $fixture_counter = 0;
my $data_path = './t/data';
my $fixture_path = "$data_path/fixtures";

my $run7915_lane5_tag0 = '7915_5#0';
my $run7915_lane5_tag1 = '7915_5#1';

my $run15440_lane1_tag0 = '15440_1#0';
my $run15440_lane1_tag81 = '15440_1#81';

my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;
my $samtools = `which samtools`;

my $have_admin_rights =
  system(qq{$WTSI::NPG::iRODS::IADMIN lu >/dev/null 2>&1}) == 0;

# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';

# Filter for recognising test groups
my $group_filter = sub {
  my ($group) = @_;
  if ($group =~ m{^$group_prefix}) {
    return 1
  }
  else {
    return 0;
  }
};

# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ } (10, 100, 619, 3720);
# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

# Test ML warehouse
my $schema;
my $db_dir = File::Temp->newdir;
# my $db_dir = ".";
my $db_file = File::Spec->catfile($db_dir, 'ml_warehouse.db');
{
  # create_test_db produces warnings during expected use, which
  # appear mixed with test output in the terminal
  local $SIG{__WARN__} = sub { };
  $schema = TestDB->new->create_test_db('WTSI::DNAP::Warehouse::Schema',
                                        './t/data/fixtures', $db_file);
}

# Reference filter for recognising the test reference
my $ref_regex = qr{\./t\/data\/test_ref.fa}msx;
my $ref_filter = sub {
  my ($line) = @_;
  return $line =~ m{$ref_regex}msx;
};

my $pid = $$;

sub setup_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("HTSFileDataObjectTest.$pid.$fixture_counter");
  $fixture_counter++;

  my $group_count = 0;
  foreach my $group (@irods_groups) {
    if (not $irods->group_exists($group)) {
      if ($have_admin_rights) {
        push @groups_added, $irods->add_group($group);
      }
    }
    else {
      $group_count++;
    }
  }

  if ($group_count == scalar @irods_groups) {
    $group_tests_enabled = 1;
  }

  if ($samtools) {
    foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1,
                           $run15440_lane1_tag0, $run15440_lane1_tag81) {
      WTSI::NPG::HTS::Samtools->new
          (arguments => ['view', '-C',
                         '-T', qq[$data_path/$reference_file],
                         '-o', qq[irods:$irods_tmp_coll/$data_file.cram]],
           path      => "$data_path/$data_file.sam")->run;

      WTSI::NPG::HTS::Samtools->new
          (arguments => ['view', '-b',
                         '-T', qq[$data_path/$reference_file],
                         '-o', qq[irods:$irods_tmp_coll/$data_file.bam]],
           path      => "$data_path/$data_file.sam")->run;

      if ($group_tests_enabled) {
        # Add some test group permissions
        foreach my $format (qw(bam cram)) {
          foreach my $group (qw(ss_10 ss_100)) {
            $irods->set_object_permissions
              ('read', $group, "$irods_tmp_coll/$data_file.$format");
          }
        }
      }
    }
  }
}

sub teardown_fixture : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods->remove_collection($irods_tmp_coll);

  if ($have_admin_rights) {
    foreach my $group (@groups_added) {
      if ($irods->group_exists($group)) {
        $irods->remove_group($group);
      }
    }
  }
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::HTSFileDataObject');
}

my @example_paths =
  ('/seq/6345/6345_5',
   '/seq/6345/6345_5_phix',
   '/seq/6345/6345_5_phix#6',
   '/seq/6345/6345_5_nonhuman#6',
   '/seq/6345/6345_5#6',
   '/seq/6345/6345_5#6_phix');

sub id_run : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $path (@example_paths) {
    my $full_path = $path . q[.cram];
    cmp_ok(WTSI::NPG::HTS::HTSFileDataObject->new($irods, $full_path)->id_run,
           '==', 6345, "$full_path id_run is correct");
  }
}

sub position : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    foreach my $path (@example_paths) {
      my $full_path = $path . ".$format";
      cmp_ok(WTSI::NPG::HTS::HTSFileDataObject->new
             ($irods, $full_path)->position,
             '==', 5, "$full_path position is correct");
    }
  }
}

sub tag_index : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    my @objs;
    foreach my $path (@example_paths) {
      push @objs, WTSI::NPG::HTS::HTSFileDataObject->new
        ($irods, $path . ".$format");
    }
    my @tag_indices = (undef, undef, 6, 6, 6, 6);

    my $iter = each_array(@objs, @tag_indices);
    while (my ($obj, $tag_index) = $iter->()) {
      my $full_path = $obj->str;
      # 2 * 6 tests
      if (defined $tag_index) {
        cmp_ok($obj->tag_index, '==', $tag_index,
               "$full_path tag_index is correct");
      }
      else {
        isnt(defined $obj->tag_index,
             "$full_path tag_index is correct");
      }
    }
  }
}

sub align_filter : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    my @objs;
    foreach my $path (@example_paths) {
      push @objs, WTSI::NPG::HTS::HTSFileDataObject->new
        ($irods, $path . ".$format");
    }
    my @align_filters = (undef, 'phix', 'phix', 'nonhuman', undef, 'phix');

    my $iter = each_array(@objs, @align_filters);
    while (my ($obj, $filter) = $iter->()) {
      my $full_path = $obj->str;
      # 2 * 6 tests
      is($obj->align_filter, $filter, "$full_path align_filter is correct");
    }
  }
}

sub header : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
      foreach my $format (qw(bam cram)) {
        my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => "$data_file.$format",
           file_format => $format,
           id_run      => 1,
           irods       => $irods,
           position    => 1);

        # 2 * 2 * 1 tests
        ok($obj->header, "$format header can be read");

        # 2 * 2 * 1 tests
        cmp_ok(scalar @{$obj->header}, '==', 11,
               "Correct number of $format header lines") or
                 diag explain $obj->header;
      }
    }
  } # SKIP samtools
}

sub is_aligned : Test(4) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 4;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
      foreach my $format (qw(bam cram)) {
        my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => "$data_file.$format",
           file_format => $format,
           id_run      => 1,
           irods       => $irods,
           position    => 1);

        # 2 * 2 * 1 tests
        ok($obj->is_aligned, "$format data are aligned");
      }
    }
  } # SKIP samtools
}

sub reference : Test(4) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 4;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
      foreach my $format (qw(bam cram)) {
        my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => "$data_file.$format",
           file_format => $format,
           id_run      => 1,
           irods       => $irods,
           position    => 1);

        my $regex = qr{\./t\/data\/test_ref.fa}msx;
        my $filter = sub {
          my ($line) = @_;
          return $line =~ m{$regex}msx;
        };

        # 2 * 2 * 1 tests
        is($obj->reference($filter), './t/data/test_ref.fa',
           "$format reference is correct");
      }
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_no_spike_bact : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag0_expected_meta =
      [{attribute => $LIBRARY_ID,               value     => '4957423'},
       {attribute => $LIBRARY_ID,               value     => '4957424'},
       {attribute => $LIBRARY_ID,               value     => '4957425'},
       {attribute => $LIBRARY_ID,               value     => '4957426'},
       {attribute => $LIBRARY_ID,               value     => '4957427'},
       {attribute => $LIBRARY_ID,               value     => '4957428'},
       {attribute => $LIBRARY_ID,               value     => '4957429'},
       {attribute => $LIBRARY_ID,               value     => '4957430'},
       {attribute => $LIBRARY_ID,               value     => '4957431'},
       {attribute => $LIBRARY_ID,               value     => '4957432'},
       {attribute => $LIBRARY_ID,               value     => '4957433'},
       {attribute => $LIBRARY_ID,               value     => '4957434'},
       {attribute => $LIBRARY_ID,               value     => '4957435'},
       {attribute => $LIBRARY_ID,               value     => '4957436'},
       {attribute => $LIBRARY_ID,               value     => '4957437'},
       {attribute => $LIBRARY_ID,               value     => '4957438'},
       {attribute => $LIBRARY_ID,               value     => '4957439'},
       {attribute => $LIBRARY_ID,               value     => '4957440'},
       {attribute => $LIBRARY_ID,               value     => '4957441'},
       {attribute => $LIBRARY_ID,               value     => '4957442'},
       {attribute => $LIBRARY_ID,               value     => '4957443'},
       {attribute => $LIBRARY_ID,               value     => '4957444'},
       {attribute => $LIBRARY_ID,               value     => '4957445'},
       {attribute => $LIBRARY_ID,               value     => '4957446'},
       {attribute => $LIBRARY_ID,               value     => '4957447'},
       {attribute => $LIBRARY_ID,               value     => '4957448'},
       {attribute => $LIBRARY_ID,               value     => '4957449'},
       {attribute => $LIBRARY_ID,               value     => '4957450'},
       {attribute => $LIBRARY_ID,               value     => '4957451'},
       {attribute => $LIBRARY_ID,               value     => '4957452'},
       {attribute => $LIBRARY_ID,               value     => '4957453'},
       {attribute => $LIBRARY_ID,               value     => '4957454'},
       {attribute => $LIBRARY_ID,               value     => '4957455'},
       {attribute => $QC_STATE,                 value     => '1'},
       {attribute => $SAMPLE_NAME,              value     => '619s040'},
       {attribute => $SAMPLE_NAME,              value     => '619s041'},
       {attribute => $SAMPLE_NAME,              value     => '619s042'},
       {attribute => $SAMPLE_NAME,              value     => '619s043'},
       {attribute => $SAMPLE_NAME,              value     => '619s044'},
       {attribute => $SAMPLE_NAME,              value     => '619s045'},
       {attribute => $SAMPLE_NAME,              value     => '619s046'},
       {attribute => $SAMPLE_NAME,              value     => '619s047'},
       {attribute => $SAMPLE_NAME,              value     => '619s048'},
       {attribute => $SAMPLE_NAME,              value     => '619s049'},
       {attribute => $SAMPLE_NAME,              value     => '619s050'},
       {attribute => $SAMPLE_NAME,              value     => '619s051'},
       {attribute => $SAMPLE_NAME,              value     => '619s052'},
       {attribute => $SAMPLE_NAME,              value     => '619s053'},
       {attribute => $SAMPLE_NAME,              value     => '619s054'},
       {attribute => $SAMPLE_NAME,              value     => '619s055'},
       {attribute => $SAMPLE_NAME,              value     => '619s056'},
       {attribute => $SAMPLE_NAME,              value     => '619s057'},
       {attribute => $SAMPLE_NAME,              value     => '619s058'},
       {attribute => $SAMPLE_NAME,              value     => '619s059'},
       {attribute => $SAMPLE_NAME,              value     => '619s060'},
       {attribute => $SAMPLE_NAME,              value     => '619s061'},
       {attribute => $SAMPLE_NAME,              value     => '619s062'},
       {attribute => $SAMPLE_NAME,              value     => '619s063'},
       {attribute => $SAMPLE_NAME,              value     => '619s064'},
       {attribute => $SAMPLE_NAME,              value     => '619s065'},
       {attribute => $SAMPLE_NAME,              value     => '619s066'},
       {attribute => $SAMPLE_NAME,              value     => '619s067'},
       {attribute => $SAMPLE_NAME,              value     => '619s068'},
       {attribute => $SAMPLE_NAME,              value     => '619s069'},
       {attribute => $SAMPLE_NAME,              value     => '619s070'},
       {attribute => $SAMPLE_NAME,              value     => '619s071'},
       {attribute => $SAMPLE_NAME,              value     => '619s072'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '10/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '109/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '15/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '17/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '21/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '23/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '35/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '4009-19'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '4033-10'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '457/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '488.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '490.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '497/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '504/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '6/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '77/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '78/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '79/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'D107310-3154'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'D68346-3058'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB30729/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB61091/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DC'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DR08726/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DR13450/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'EM10266/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'EM2107'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'I64043-3096'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'K11277244-293'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'P73230-3018'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'SOIL'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
       {attribute => $STUDY_ID,                 value     => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity'}];

    my $spiked_control = 0;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run7915_lane5_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_spike_bact : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag0_expected_meta =
      [{attribute => $LIBRARY_ID,               value     => '3691209'}, # spike
       {attribute => $LIBRARY_ID,               value     => '4957423'},
       {attribute => $LIBRARY_ID,               value     => '4957424'},
       {attribute => $LIBRARY_ID,               value     => '4957425'},
       {attribute => $LIBRARY_ID,               value     => '4957426'},
       {attribute => $LIBRARY_ID,               value     => '4957427'},
       {attribute => $LIBRARY_ID,               value     => '4957428'},
       {attribute => $LIBRARY_ID,               value     => '4957429'},
       {attribute => $LIBRARY_ID,               value     => '4957430'},
       {attribute => $LIBRARY_ID,               value     => '4957431'},
       {attribute => $LIBRARY_ID,               value     => '4957432'},
       {attribute => $LIBRARY_ID,               value     => '4957433'},
       {attribute => $LIBRARY_ID,               value     => '4957434'},
       {attribute => $LIBRARY_ID,               value     => '4957435'},
       {attribute => $LIBRARY_ID,               value     => '4957436'},
       {attribute => $LIBRARY_ID,               value     => '4957437'},
       {attribute => $LIBRARY_ID,               value     => '4957438'},
       {attribute => $LIBRARY_ID,               value     => '4957439'},
       {attribute => $LIBRARY_ID,               value     => '4957440'},
       {attribute => $LIBRARY_ID,               value     => '4957441'},
       {attribute => $LIBRARY_ID,               value     => '4957442'},
       {attribute => $LIBRARY_ID,               value     => '4957443'},
       {attribute => $LIBRARY_ID,               value     => '4957444'},
       {attribute => $LIBRARY_ID,               value     => '4957445'},
       {attribute => $LIBRARY_ID,               value     => '4957446'},
       {attribute => $LIBRARY_ID,               value     => '4957447'},
       {attribute => $LIBRARY_ID,               value     => '4957448'},
       {attribute => $LIBRARY_ID,               value     => '4957449'},
       {attribute => $LIBRARY_ID,               value     => '4957450'},
       {attribute => $LIBRARY_ID,               value     => '4957451'},
       {attribute => $LIBRARY_ID,               value     => '4957452'},
       {attribute => $LIBRARY_ID,               value     => '4957453'},
       {attribute => $LIBRARY_ID,               value     => '4957454'},
       {attribute => $LIBRARY_ID,               value     => '4957455'},
       {attribute => $QC_STATE,                 value     => '1'},
       {attribute => $SAMPLE_NAME,              value     => '619s040'},
       {attribute => $SAMPLE_NAME,              value     => '619s041'},
       {attribute => $SAMPLE_NAME,              value     => '619s042'},
       {attribute => $SAMPLE_NAME,              value     => '619s043'},
       {attribute => $SAMPLE_NAME,              value     => '619s044'},
       {attribute => $SAMPLE_NAME,              value     => '619s045'},
       {attribute => $SAMPLE_NAME,              value     => '619s046'},
       {attribute => $SAMPLE_NAME,              value     => '619s047'},
       {attribute => $SAMPLE_NAME,              value     => '619s048'},
       {attribute => $SAMPLE_NAME,              value     => '619s049'},
       {attribute => $SAMPLE_NAME,              value     => '619s050'},
       {attribute => $SAMPLE_NAME,              value     => '619s051'},
       {attribute => $SAMPLE_NAME,              value     => '619s052'},
       {attribute => $SAMPLE_NAME,              value     => '619s053'},
       {attribute => $SAMPLE_NAME,              value     => '619s054'},
       {attribute => $SAMPLE_NAME,              value     => '619s055'},
       {attribute => $SAMPLE_NAME,              value     => '619s056'},
       {attribute => $SAMPLE_NAME,              value     => '619s057'},
       {attribute => $SAMPLE_NAME,              value     => '619s058'},
       {attribute => $SAMPLE_NAME,              value     => '619s059'},
       {attribute => $SAMPLE_NAME,              value     => '619s060'},
       {attribute => $SAMPLE_NAME,              value     => '619s061'},
       {attribute => $SAMPLE_NAME,              value     => '619s062'},
       {attribute => $SAMPLE_NAME,              value     => '619s063'},
       {attribute => $SAMPLE_NAME,              value     => '619s064'},
       {attribute => $SAMPLE_NAME,              value     => '619s065'},
       {attribute => $SAMPLE_NAME,              value     => '619s066'},
       {attribute => $SAMPLE_NAME,              value     => '619s067'},
       {attribute => $SAMPLE_NAME,              value     => '619s068'},
       {attribute => $SAMPLE_NAME,              value     => '619s069'},
       {attribute => $SAMPLE_NAME,              value     => '619s070'},
       {attribute => $SAMPLE_NAME,              value     => '619s071'},
       {attribute => $SAMPLE_NAME,              value     => '619s072'},
       {attribute => $SAMPLE_NAME,
        value     => "phiX_for_spiked_buffers"},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '10/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '109/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '15/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '17/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '21/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '23/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '35/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '4009-19'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '4033-10'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '457/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '488.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '490.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '497/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '504/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '6/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '77/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '78/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '79/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'D107310-3154'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'D68346-3058'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB30729/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DB61091/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DC'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DR08726/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'DR13450/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'EM10266/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'EM2107'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'I64043-3096'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'K11277244-293'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'P73230-3018'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => 'SOIL'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_NAME,
        value     => 'Illumina Controls'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
       {attribute => $STUDY_ID,                 value     => '198'},
       {attribute => $STUDY_ID,                 value     => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity'}];

    my $spiked_control = 1;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run7915_lane5_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_198',
                                                       'ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag1_no_spike_bact : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag1_expected_meta =
      [{attribute => $LIBRARY_ID,               value     => '4957423'},
       {attribute => $QC_STATE,                 value     => '1'},
       {attribute => $SAMPLE_NAME,              value     => '619s040'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
       {attribute => $STUDY_ID,                 value     => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity'}];

    my $spiked_control = 0;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run7915_lane5_tag1,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag1_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag1_spike_bact : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag1_expected_meta =
      [{attribute => $LIBRARY_ID,               value     => '4957423'},
       {attribute => $QC_STATE,                 value     => '1'},
       {attribute => $SAMPLE_NAME,              value     => '619s040'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value     => '153.0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value     => 'ERP000251'},
       {attribute => $STUDY_ID,                 value     => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity'}];

    my $spiked_control = 1;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run7915_lane5_tag1,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag1_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_no_spike_human : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag0_expected_meta =
      [{attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_ID,               value => '12789802'},
       {attribute => $LIBRARY_ID,               value => '12789814'},
       {attribute => $LIBRARY_ID,               value => '12789826'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759045'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759048'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759062'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759045'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759048'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759062'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '6AJ182'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8AJ1'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8R163'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP005180'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq'}];

    my $spiked_control = 0;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run15440_lane1_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_spike_human : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag0_expected_meta =
      [{attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_ID,               value => '12789802'},
       {attribute => $LIBRARY_ID,               value => '12789814'},
       {attribute => $LIBRARY_ID,               value => '12789826'},
       {attribute => $LIBRARY_ID,               value => '6759268'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759045'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759048'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759062'},
       {attribute => $SAMPLE_NAME,
        value     => 'phiX_for_spiked_buffers'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759045'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759048'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759062'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '6AJ182'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8AJ1'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8R163'},
       {attribute => $STUDY_NAME,               value => 'Illumina Controls'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP005180'},
       {attribute => $STUDY_ID,                 value => '198'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq'}];

    my $spiked_control = 1;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run15440_lane1_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_198',
                                                       'ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag81_no_spike_human : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag81_expected_meta =
      [{attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP005180'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq'}];

    my $spiked_control = 0;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run15440_lane1_tag81,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag81_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag81_spike_human : Test(8) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 8;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter);

    my $tag81_expected_meta =
      [{attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP005180'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq'}];

    my $spiked_control = 1;

    foreach my $format (qw(bam cram)) {
      # 2 * 4 tests
      test_metadata_update($irods, $irods_tmp_coll, $schema, $ref_filter,
                           {data_file              => $run15440_lane1_tag81,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag81_expected_meta,
                            expected_groups_before => ['ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub test_metadata_update {
  my ($irods, $working_coll, $schema, $ref_filter, $args) = @_;

  ref $args eq 'HASH' or croak "The arguments must be a HashRef";

  my $data_file      = $args->{data_file};
  my $format         = $args->{format};
  my $spiked         = $args->{spiked_control};
  my $exp_metadata   = $args->{expected_metadata};
  my $exp_grp_before = $args->{expected_groups_before};
  my $exp_grp_after  = $args->{expected_groups_after};

  my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
    (collection  => $working_coll,
     data_object => "$data_file.$format",,
     irods       => $irods);
  my $tag = $obj->tag_index;

  my @groups_before = $obj->get_groups;
  ok($obj->update_secondary_metadata($schema, $spiked, $ref_filter),
     "Secondary metadata ran; format: $format, tag: $tag, spiked: $spiked");
  my @groups_after = $obj->get_groups;

  my $metadata = $obj->metadata;
  is_deeply($metadata, $exp_metadata,
            "Secondary metadata was updated; format: $format, " .
            "tag: $tag, spiked: $spiked")
    or diag explain $metadata;

   SKIP: {
       if (not $group_tests_enabled) {
         skip 'iRODS test groups were not present', 2;
       }
       else {
         is_deeply(\@groups_before, $exp_grp_before,
                   'Groups before update') or diag explain \@groups_before;

         is_deeply(\@groups_after, $exp_grp_after,
                   'Groups after update') or diag explain \@groups_after;
       }
     } # SKIP groups_added
}

1;
