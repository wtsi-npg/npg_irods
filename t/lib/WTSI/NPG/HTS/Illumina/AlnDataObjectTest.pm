package WTSI::NPG::HTS::Illumina::AlnDataObjectTest;

use utf8;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::DNAP::Utilities::Runnable;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;
use npg_tracking::glossary::composition;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

{
  package TestAnnotator;
  use Moose;

  with 'WTSI::NPG::HTS::Illumina::Annotator';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/aln_data_object';
my $fixture_path = "./t/fixtures";

my $utf8_extra = '[UTF-8 test: Τὴ γλῶσσα μοῦ ἔδωσαν ἑλληνικὴ το σπίτι φτωχικό στις αμμουδιές του Ομήρου.]';

my $db_dir = File::Temp->newdir;
my $wh_schema;
my $lims_factory;

my $run7915_lane5_tag0       = '7915_5#0';
my $run7915_lane5_tag1       = '7915_5#1';
my $run7915_lane5_tag1_human = '7915_5#1_human';

my $run15440_lane1_tag0  = '15440_1#0';
my $run15440_lane1_tag81 = '15440_1#81';

my %file_composition =
  ('7915_5#0'           => [7915,  5,  0,      undef],
   '7915_5#1'           => [7915,  5,  1,      undef],
   '7915_5#1_human'     => [7915,  5,  1,     'human'],
   '15440_1#0'          => [15440, 1,  0,      undef],
   '15440_1#81'         => [15440, 1, 81,      undef],

   '17550_3#1'          => [17550, 3,  1,      undef],
   '17550_3#1_human'    => [17550, 3,  1,    'human'],
   '17550_3#1_nonhuman' => [17550, 3,  1, 'nonhuman'],
   '17550_3#1_xahuman'  => [17550, 3,  1,  'xahuman'],
   '17550_3#1_yhuman'   => [17550, 3,  1,   'yhuman'],
   '17550_3#1_phix'     => [17550, 3,  1,     'phix']);

my $invalid = "1000_1#1";

my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;
my $samtools_available = `which samtools`;

my $have_admin_rights =
  system(qq[$WTSI::NPG::iRODS::IADMIN lu >/dev/null 2>&1]) == 0;

# The public group
my $public_group = 'public';
# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';

# Filter for recognising test groups
my $group_filter = sub {
  my ($group) = @_;
  if ($group eq $public_group or $group =~ m{^$group_prefix}) {
    return 1
  }
  else {
    return 0;
  }
};

# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ , $group_prefix . $_ . '_human' }
                   (10, 100, 198, 619, 2967, 3720);
push @irods_groups, $public_group;
# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

# Reference filter for recognising the test reference
my $ref_regex = qr{\./t\/data\/aln_data_object\/test_ref.fa}msx;
my $ref_filter = sub {
  my ($line) = @_;
  return $line =~ m{$ref_regex}msx;
};

sub setup_databases : Test(startup) {
  my $wh_db_file = catfile($db_dir, 'ml_wh.db');
  $wh_schema = TestDB->new(sqlite_utf8_enabled => 1,
                           verbose             => 0)->create_test_db
    ('WTSI::DNAP::Warehouse::Schema', "$fixture_path/ml_warehouse",
     $wh_db_file);

  $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(mlwh_schema => $wh_schema);
}

sub teardown_databases : Test(shutdown) {
  $wh_schema->storage->disconnect;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("Illumina::AlnDataObjectTest.$pid.$test_counter");
  $test_counter++;

  my $group_count = 0;
  foreach my $group (@irods_groups) {
    if ($irods->group_exists($group)) {
      $group_count++;
    }
    else {
      if ($have_admin_rights) {
        push @groups_added, $irods->add_group($group);
        $group_count++;
      }
    }
  }

  if ($group_count == scalar @irods_groups) {
    $group_tests_enabled = 1;
  }

  if ($samtools_available) {
    foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1,
                           $run7915_lane5_tag1_human,
                           $run15440_lane1_tag0, $run15440_lane1_tag81) {
      WTSI::DNAP::Utilities::Runnable->new
          (arguments  => ['view', '-C',
                          '-T', "$data_path/$reference_file",
                          '-o', "irods:$irods_tmp_coll/$data_file.cram",
                                "$data_path/$data_file.sam"],
           executable => 'samtools')->run;
      WTSI::DNAP::Utilities::Runnable->new
          (arguments  => ['view', '-b',
                          '-T', "$data_path/$reference_file",
                          '-o', "irods:$irods_tmp_coll/$data_file.bam",
                                "$data_path/$data_file.sam"],
           executable => 'samtools')->run;
      my @initargs = _build_initargs(\%file_composition, $data_file);

      foreach my $format (qw[bam cram]) {
        my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => "$data_file.$format",
           irods       => $irods,
           @initargs);

        my $num_reads = 10000;

        my $composition = $obj->composition;
        my $component = $composition->get_component(0);
        my %args = (
           is_paired_read => 1,
           is_aligned     => $obj->is_aligned,
           num_reads      => $num_reads,
           reference      => $obj->reference, 
        );
        my $ta = TestAnnotator->new();

        my @avus;
        push @avus, $ta->make_primary_metadata($composition, %args),
          $ta->make_alignment_metadata(
            $component, $num_reads, $obj->reference, $obj->is_aligned);

        foreach my $avu (@avus) {
          my $attribute = $avu->{attribute};
          my $value     = $avu->{value};
          my $units     = $avu->{units};
          $obj->supersede_avus($attribute, $value, $units);
        }

        # Add some test group permissions
        if ($group_tests_enabled) {
          $obj->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                $public_group);

          foreach my $group (map { $group_prefix . $_ } (10, 100)) {
            $obj->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                  $group);
          }
        }
      }
    }
  }
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
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
  require_ok('WTSI::NPG::HTS::Illumina::AlnDataObject');
}

sub id_run : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /17550/ } keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      cmp_ok(WTSI::NPG::HTS::Illumina::AlnDataObject->new
             ($irods, $full_path, @initargs)->id_run,
             '==', 17550, "$full_path id_run is correct");
    }
  }
}

sub position : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /17550/ } keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      cmp_ok(WTSI::NPG::HTS::Illumina::AlnDataObject->new
             ($irods, $full_path, @initargs)->position,
             '==', 3, "$full_path position is correct");
    }
  }
}

sub is_restricted_access : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # Without any study metadata information
  foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /17550/ } keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
        ($irods, $full_path, @initargs);
      ok($obj->is_restricted_access, "$full_path is restricted_access");
    }
  }
}

sub is_paired_read : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
 foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /15440/ } keys %file_composition) {
      my $full_path = "$irods_tmp_coll/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
        ($irods, $full_path, @initargs);

      my $index = $obj->tag_index;
      if ($index == 0) {
        ok($obj->is_paired_read, "15440_1.$format index $index is paired");
      }
      elsif ($index == 81) {
        ok(!$obj->is_paired_read, "15440_1.$format index $index is not paired");
      }
      else {
        fail "Unexpected tag index $index";
      }
    }
  }
}

sub tag_index : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /17550/ } keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      cmp_ok(WTSI::NPG::HTS::Illumina::AlnDataObject->new
             ($irods, $full_path, @initargs)->tag_index,
             '==', 1, "$full_path tag_index is correct");
    }
  }
}

sub subset : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (qw[bam cram]) {
    foreach my $path (sort grep { /17550/ } keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      my ($expected) = $path =~ m{_((human|nonhuman|xahuman|yhuman|phix))};
      my $subset = WTSI::NPG::HTS::Illumina::AlnDataObject->new
        ($irods, $full_path, @initargs)->subset;

      is($subset, $expected, "$full_path subset is correct");
    }
  }
}

sub nonconsented_human_access_revoked : Test(6) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 1;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json =
      '{"components":[{"id_run":7915,"position":5,"subset":"human","tag_index":1}]}';
    my $tag1_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => 'alignment_filter',        value => 'human'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":7915,"position":5,"subset":"human","tag_index":1}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '7915'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '5'},
       {attribute => $LIBRARY_ID,               value => '4957423'},
       {attribute => $LIBRARY_TYPE,             value => 'No PCR'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => '619s040'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'ERS012323'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,                value => '230889'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '153.0'},
       {attribute => $SAMPLE_UUID,              value => 'a8b4ebaa-c628-11df-8e7f-00144f2062b0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP000251'},
       {attribute => $STUDY_ID,                 value => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '1'},
       {attribute => $TARGET,                   value => '0'},
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 1;

    test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                         {data_file              => $run7915_lane5_tag1_human,
                          format                 => 'cram',
                          spiked_control         => $spiked_control,
                          expected_metadata      => $tag1_expected_meta,
                          expected_groups_before => [$public_group,
                                                     'ss_10',
                                                     'ss_100'],
                          expected_groups_after  => ['ss_619_human']
                         });
  }
}

sub header : Test(13) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 13;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);
    my $pkg = 'npg_tracking::glossary::composition::component::illumina';

    foreach my $format (qw[bam cram]) {
      foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
        my $file_name = "$data_file.$format";
        my @initargs = _build_initargs(\%file_composition, $data_file);

        my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => $file_name,
           file_format => $format,
           irods       => $irods,
           @initargs);

        # 2 * 2 * 1 tests
        ok($obj->header, "$format header can be read");

        # 2 * 2 * 2 tests
        my $num_lines = scalar @{$obj->header};
        $num_lines ||= 0;
        # Starting from samtools v. 1.10.0, a @PG line is inserted
        # into the header for each samtools invocation. The original
        # number of header lines is 11, at least one more samtools
        # command is invoked during the tests (getting the header
        # itself). With older versions of samtools 11 header lines
        # will be still present at this point.
        ok(($num_lines >= 11),
          "Number of $format header lines is the same as in original " .
          'test data or more') or diag explain $obj->header;
        $num_lines = scalar grep { /\@PG/ } @{$obj->header};
        $num_lines ||= 0;
        ok(($num_lines >= 1),
          "At least one \@PG line exists in $format header") or
          diag explain $obj->header;
      }
    }

    $irods->add_object("$data_path/$invalid.cram",
                       "$irods_tmp_coll/$invalid.cram",
                       $WTSI::NPG::iRODS::CALC_CHECKSUM);

    # Ensure that a malformed file raises an exception
    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
      (collection  => $irods_tmp_coll,
       data_object => "$invalid.cram",
       id_run      => 1000,
       position    => 1,
       tag_index   => 0,
       file_format => 'cram',
       irods       => $irods);

    dies_ok { $obj->header }
      'Expected failure reading header of invalid cram file';
  } # SKIP samtools
}

sub is_aligned : Test(4) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 4;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    foreach my $format (qw[bam cram]) {
      foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
        my $file_name = "$data_file.$format";
        my @initargs = _build_initargs(\%file_composition, $data_file);

        my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => $file_name,
           file_format => $format,
           irods       => $irods,
           @initargs);

        # 2 * 2 * 1 tests
        ok($obj->is_aligned, "$format data are aligned");
      }
    }
  } # SKIP samtools
}

sub reference : Test(4) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 4;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    foreach my $format (qw[bam cram]) {
      foreach my $data_file ($run7915_lane5_tag0, $run7915_lane5_tag1) {
        my $file_name = "$data_file.$format";
        my @initargs = _build_initargs(\%file_composition, $data_file);

        my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
          (collection  => $irods_tmp_coll,
           data_object => $file_name,
           file_format => $format,
           irods       => $irods,
           @initargs);

        # 2 * 2 * 1 tests
        is($obj->reference($ref_filter), "$data_path/test_ref.fa",
           "$format reference is correct");
      }
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_no_spike_bact : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0,);

    my $c_json = '{"components":[{"id_run":7915,"position":5,"tag_index":0}]}';
    my $tag0_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":7915,"position":5,"tag_index":0}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
	value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '7915'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '5'},
       {attribute => $LIBRARY_ID,               value => '4957423'},
       {attribute => $LIBRARY_ID,               value => '4957424'},
       {attribute => $LIBRARY_ID,               value => '4957425'},
       {attribute => $LIBRARY_ID,               value => '4957426'},
       {attribute => $LIBRARY_ID,               value => '4957427'},
       {attribute => $LIBRARY_ID,               value => '4957428'},
       {attribute => $LIBRARY_ID,               value => '4957429'},
       {attribute => $LIBRARY_ID,               value => '4957430'},
       {attribute => $LIBRARY_ID,               value => '4957431'},
       {attribute => $LIBRARY_ID,               value => '4957432'},
       {attribute => $LIBRARY_ID,               value => '4957433'},
       {attribute => $LIBRARY_ID,               value => '4957434'},
       {attribute => $LIBRARY_ID,               value => '4957435'},
       {attribute => $LIBRARY_ID,               value => '4957436'},
       {attribute => $LIBRARY_ID,               value => '4957437'},
       {attribute => $LIBRARY_ID,               value => '4957438'},
       {attribute => $LIBRARY_ID,               value => '4957439'},
       {attribute => $LIBRARY_ID,               value => '4957440'},
       {attribute => $LIBRARY_ID,               value => '4957441'},
       {attribute => $LIBRARY_ID,               value => '4957442'},
       {attribute => $LIBRARY_ID,               value => '4957443'},
       {attribute => $LIBRARY_ID,               value => '4957444'},
       {attribute => $LIBRARY_ID,               value => '4957445'},
       {attribute => $LIBRARY_ID,               value => '4957446'},
       {attribute => $LIBRARY_ID,               value => '4957447'},
       {attribute => $LIBRARY_ID,               value => '4957448'},
       {attribute => $LIBRARY_ID,               value => '4957449'},
       {attribute => $LIBRARY_ID,               value => '4957450'},
       {attribute => $LIBRARY_ID,               value => '4957451'},
       {attribute => $LIBRARY_ID,               value => '4957452'},
       {attribute => $LIBRARY_ID,               value => '4957453'},
       {attribute => $LIBRARY_ID,               value => '4957454'},
       {attribute => $LIBRARY_ID,               value => '4957455'},
       {attribute => $LIBRARY_TYPE,             value => 'No PCR'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => '619s040'},
       {attribute => $SAMPLE_NAME,              value => '619s041'},
       {attribute => $SAMPLE_NAME,              value => '619s042'},
       {attribute => $SAMPLE_NAME,              value => '619s043'},
       {attribute => $SAMPLE_NAME,              value => '619s044'},
       {attribute => $SAMPLE_NAME,              value => '619s045'},
       {attribute => $SAMPLE_NAME,              value => '619s046'},
       {attribute => $SAMPLE_NAME,              value => '619s047'},
       {attribute => $SAMPLE_NAME,              value => '619s048'},
       {attribute => $SAMPLE_NAME,              value => '619s049'},
       {attribute => $SAMPLE_NAME,              value => '619s050'},
       {attribute => $SAMPLE_NAME,              value => '619s051'},
       {attribute => $SAMPLE_NAME,              value => '619s052'},
       {attribute => $SAMPLE_NAME,              value => '619s053'},
       {attribute => $SAMPLE_NAME,              value => '619s054'},
       {attribute => $SAMPLE_NAME,              value => '619s055'},
       {attribute => $SAMPLE_NAME,              value => '619s056'},
       {attribute => $SAMPLE_NAME,              value => '619s057'},
       {attribute => $SAMPLE_NAME,              value => '619s058'},
       {attribute => $SAMPLE_NAME,              value => '619s059'},
       {attribute => $SAMPLE_NAME,              value => '619s060'},
       {attribute => $SAMPLE_NAME,              value => '619s061'},
       {attribute => $SAMPLE_NAME,              value => '619s062'},
       {attribute => $SAMPLE_NAME,              value => '619s063'},
       {attribute => $SAMPLE_NAME,              value => '619s064'},
       {attribute => $SAMPLE_NAME,              value => '619s065'},
       {attribute => $SAMPLE_NAME,              value => '619s066'},
       {attribute => $SAMPLE_NAME,              value => '619s067'},
       {attribute => $SAMPLE_NAME,              value => '619s068'},
       {attribute => $SAMPLE_NAME,              value => '619s069'},
       {attribute => $SAMPLE_NAME,              value => '619s070'},
       {attribute => $SAMPLE_NAME,              value => '619s071'},
       {attribute => $SAMPLE_NAME,              value => '619s072'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012323'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012324'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012325'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012326'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012327'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012328'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012329'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012330'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012331'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012332'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012333'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012334'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012335'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012336'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012337'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012338'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012339'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012340'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012341'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012342'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012343'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012344'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012345'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012346'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012347'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012348'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012349'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012350'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012351'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012352'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012353'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012354'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012355'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,                value => '230889'},
       {attribute => $SAMPLE_ID,                value => '230890'},
       {attribute => $SAMPLE_ID,                value => '230891'},
       {attribute => $SAMPLE_ID,                value => '230892'},
       {attribute => $SAMPLE_ID,                value => '230893'},
       {attribute => $SAMPLE_ID,                value => '230894'},
       {attribute => $SAMPLE_ID,                value => '230895'},
       {attribute => $SAMPLE_ID,                value => '230896'},
       {attribute => $SAMPLE_ID,                value => '230897'},
       {attribute => $SAMPLE_ID,                value => '230898'},
       {attribute => $SAMPLE_ID,                value => '230899'},
       {attribute => $SAMPLE_ID,                value => '230900'},
       {attribute => $SAMPLE_ID,                value => '230901'},
       {attribute => $SAMPLE_ID,                value => '230902'},
       {attribute => $SAMPLE_ID,                value => '230903'},
       {attribute => $SAMPLE_ID,                value => '230904'},
       {attribute => $SAMPLE_ID,                value => '230905'},
       {attribute => $SAMPLE_ID,                value => '230906'},
       {attribute => $SAMPLE_ID,                value => '230907'},
       {attribute => $SAMPLE_ID,                value => '230908'},
       {attribute => $SAMPLE_ID,                value => '230909'},
       {attribute => $SAMPLE_ID,                value => '230910'},
       {attribute => $SAMPLE_ID,                value => '230911'},
       {attribute => $SAMPLE_ID,                value => '230912'},
       {attribute => $SAMPLE_ID,                value => '230913'},
       {attribute => $SAMPLE_ID,                value => '230914'},
       {attribute => $SAMPLE_ID,                value => '230915'},
       {attribute => $SAMPLE_ID,                value => '230916'},
       {attribute => $SAMPLE_ID,                value => '230917'},
       {attribute => $SAMPLE_ID,                value => '230918'},
       {attribute => $SAMPLE_ID,                value => '230919'},
       {attribute => $SAMPLE_ID,                value => '230920'},
       {attribute => $SAMPLE_ID,                value => '230921'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '10/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '109/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '15/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '153.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '17/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '21/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '23/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '35/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '4009-19'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '4033-10'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '457/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '488.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '490.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '497/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '504/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '6/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '77/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '78/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '79/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'D107310-3154'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'D68346-3058'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DB'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DB30729/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DB61091/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DC'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DR08726/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'DR13450/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'EM10266/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'EM2107'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'I64043-3096'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'K11277244-293'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'P73230-3018'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'SOIL'},
       {attribute => $SAMPLE_UUID,              value => 'a8b4ebaa-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4f3ac-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4fbae-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b503a6-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b50b9e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b51396-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b51ba2-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5239a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b52b92-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5338a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b53b82-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5437a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b54b7c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b55374-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b55b94-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5638c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b56bb6-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b573ae-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b57bb0-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b583a8-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b58b96-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5938e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b59b86-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5a37e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5ab6c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5b364-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5bb5c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5c35e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5cb4c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5d34e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5db46-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5e352-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5eb54-c628-11df-8e7f-00144f2062b0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP000251'},
       {attribute => $STUDY_ID,                 value => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '0'},
       {attribute => $TARGET,                   value => '0'}, # target 0
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 0;

    my @initargs = _build_initargs(\%file_composition, $run7915_lane5_tag0);

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run7915_lane5_tag0,
                            initargs               => \@initargs,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_spike_bact : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0,);

    my $c_json = '{"components":[{"id_run":7915,"position":5,"tag_index":0}]}';
    my $tag0_expected_meta =
      [{attribute => $ALIGNMENT,               value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":7915,"position":5,"tag_index":0}'},
       {attribute => $COMPOSITION,              value =>$c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                  value => '7915'},
       {attribute => $IS_PAIRED_READ,          value => '1'},
       {attribute => $POSITION,                value => '5'},
       {attribute => $LIBRARY_ID,              value => '3691209'}, # spike
       {attribute => $LIBRARY_ID,              value => '4957423'},
       {attribute => $LIBRARY_ID,              value => '4957424'},
       {attribute => $LIBRARY_ID,              value => '4957425'},
       {attribute => $LIBRARY_ID,              value => '4957426'},
       {attribute => $LIBRARY_ID,              value => '4957427'},
       {attribute => $LIBRARY_ID,              value => '4957428'},
       {attribute => $LIBRARY_ID,              value => '4957429'},
       {attribute => $LIBRARY_ID,              value => '4957430'},
       {attribute => $LIBRARY_ID,              value => '4957431'},
       {attribute => $LIBRARY_ID,              value => '4957432'},
       {attribute => $LIBRARY_ID,              value => '4957433'},
       {attribute => $LIBRARY_ID,              value => '4957434'},
       {attribute => $LIBRARY_ID,              value => '4957435'},
       {attribute => $LIBRARY_ID,              value => '4957436'},
       {attribute => $LIBRARY_ID,              value => '4957437'},
       {attribute => $LIBRARY_ID,              value => '4957438'},
       {attribute => $LIBRARY_ID,              value => '4957439'},
       {attribute => $LIBRARY_ID,              value => '4957440'},
       {attribute => $LIBRARY_ID,              value => '4957441'},
       {attribute => $LIBRARY_ID,              value => '4957442'},
       {attribute => $LIBRARY_ID,              value => '4957443'},
       {attribute => $LIBRARY_ID,              value => '4957444'},
       {attribute => $LIBRARY_ID,              value => '4957445'},
       {attribute => $LIBRARY_ID,              value => '4957446'},
       {attribute => $LIBRARY_ID,              value => '4957447'},
       {attribute => $LIBRARY_ID,              value => '4957448'},
       {attribute => $LIBRARY_ID,              value => '4957449'},
       {attribute => $LIBRARY_ID,              value => '4957450'},
       {attribute => $LIBRARY_ID,              value => '4957451'},
       {attribute => $LIBRARY_ID,              value => '4957452'},
       {attribute => $LIBRARY_ID,              value => '4957453'},
       {attribute => $LIBRARY_ID,              value => '4957454'},
       {attribute => $LIBRARY_ID,              value => '4957455'},
       {attribute => $LIBRARY_TYPE,            value => 'No PCR'},
       {attribute => $QC_STATE,                value => '1'},
       {attribute => $SAMPLE_NAME,             value => '619s040'},
       {attribute => $SAMPLE_NAME,             value => '619s041'},
       {attribute => $SAMPLE_NAME,             value => '619s042'},
       {attribute => $SAMPLE_NAME,             value => '619s043'},
       {attribute => $SAMPLE_NAME,             value => '619s044'},
       {attribute => $SAMPLE_NAME,             value => '619s045'},
       {attribute => $SAMPLE_NAME,             value => '619s046'},
       {attribute => $SAMPLE_NAME,             value => '619s047'},
       {attribute => $SAMPLE_NAME,             value => '619s048'},
       {attribute => $SAMPLE_NAME,             value => '619s049'},
       {attribute => $SAMPLE_NAME,             value => '619s050'},
       {attribute => $SAMPLE_NAME,             value => '619s051'},
       {attribute => $SAMPLE_NAME,             value => '619s052'},
       {attribute => $SAMPLE_NAME,             value => '619s053'},
       {attribute => $SAMPLE_NAME,             value => '619s054'},
       {attribute => $SAMPLE_NAME,             value => '619s055'},
       {attribute => $SAMPLE_NAME,             value => '619s056'},
       {attribute => $SAMPLE_NAME,             value => '619s057'},
       {attribute => $SAMPLE_NAME,             value => '619s058'},
       {attribute => $SAMPLE_NAME,             value => '619s059'},
       {attribute => $SAMPLE_NAME,             value => '619s060'},
       {attribute => $SAMPLE_NAME,             value => '619s061'},
       {attribute => $SAMPLE_NAME,             value => '619s062'},
       {attribute => $SAMPLE_NAME,             value => '619s063'},
       {attribute => $SAMPLE_NAME,             value => '619s064'},
       {attribute => $SAMPLE_NAME,             value => '619s065'},
       {attribute => $SAMPLE_NAME,             value => '619s066'},
       {attribute => $SAMPLE_NAME,             value => '619s067'},
       {attribute => $SAMPLE_NAME,             value => '619s068'},
       {attribute => $SAMPLE_NAME,             value => '619s069'},
       {attribute => $SAMPLE_NAME,             value => '619s070'},
       {attribute => $SAMPLE_NAME,             value => '619s071'},
       {attribute => $SAMPLE_NAME,             value => '619s072'},
       {attribute => $SAMPLE_NAME,
        value     => "phiX_for_spiked_buffers"},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012323'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012324'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012325'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012326'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012327'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012328'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012329'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012330'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012331'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012332'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012333'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012334'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012335'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012336'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012337'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012338'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012339'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012340'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012341'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012342'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012343'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012344'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012345'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012346'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012347'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012348'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012349'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012350'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012351'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012352'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012353'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012354'},
       {attribute => $SAMPLE_ACCESSION_NUMBER, value => 'ERS012355'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,               value => '1255141'}, # spike
       {attribute => $SAMPLE_ID,               value => '230889'},
       {attribute => $SAMPLE_ID,               value => '230890'},
       {attribute => $SAMPLE_ID,               value => '230891'},
       {attribute => $SAMPLE_ID,               value => '230892'},
       {attribute => $SAMPLE_ID,               value => '230893'},
       {attribute => $SAMPLE_ID,               value => '230894'},
       {attribute => $SAMPLE_ID,               value => '230895'},
       {attribute => $SAMPLE_ID,               value => '230896'},
       {attribute => $SAMPLE_ID,               value => '230897'},
       {attribute => $SAMPLE_ID,               value => '230898'},
       {attribute => $SAMPLE_ID,               value => '230899'},
       {attribute => $SAMPLE_ID,               value => '230900'},
       {attribute => $SAMPLE_ID,               value => '230901'},
       {attribute => $SAMPLE_ID,               value => '230902'},
       {attribute => $SAMPLE_ID,               value => '230903'},
       {attribute => $SAMPLE_ID,               value => '230904'},
       {attribute => $SAMPLE_ID,               value => '230905'},
       {attribute => $SAMPLE_ID,               value => '230906'},
       {attribute => $SAMPLE_ID,               value => '230907'},
       {attribute => $SAMPLE_ID,               value => '230908'},
       {attribute => $SAMPLE_ID,               value => '230909'},
       {attribute => $SAMPLE_ID,               value => '230910'},
       {attribute => $SAMPLE_ID,               value => '230911'},
       {attribute => $SAMPLE_ID,               value => '230912'},
       {attribute => $SAMPLE_ID,               value => '230913'},
       {attribute => $SAMPLE_ID,               value => '230914'},
       {attribute => $SAMPLE_ID,               value => '230915'},
       {attribute => $SAMPLE_ID,               value => '230916'},
       {attribute => $SAMPLE_ID,               value => '230917'},
       {attribute => $SAMPLE_ID,               value => '230918'},
       {attribute => $SAMPLE_ID,               value => '230919'},
       {attribute => $SAMPLE_ID,               value => '230920'},
       {attribute => $SAMPLE_ID,               value => '230921'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '10/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '109/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '15/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '153.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '17/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '21/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '23/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '35/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '4009-19'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '4033-10'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '457/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '488.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '490.0'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '497/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '504/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '6/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '77/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '78/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => '79/96'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'D107310-3154'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'D68346-3058'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DB'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DB30729/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DB61091/00'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DC'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DR08726/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'DR13450/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'EM10266/01'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'EM2107'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'I64043-3096'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'K11277244-293'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'P73230-3018'},
       {attribute => $SAMPLE_PUBLIC_NAME,      value => 'SOIL'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4ebaa-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4f3ac-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4fbae-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b503a6-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b50b9e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b51396-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b51ba2-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5239a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b52b92-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5338a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b53b82-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5437a-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b54b7c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b55374-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b55b94-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5638c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b56bb6-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b573ae-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b57bb0-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b583a8-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b58b96-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5938e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b59b86-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5a37e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5ab6c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5b364-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5bb5c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5c35e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5cb4c-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5d34e-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5db46-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5e352-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b5eb54-c628-11df-8e7f-00144f2062b0'},
							{attribute => $SAMPLE_UUID,              value => 'd3a59c4c-c037-11e0-834c-00144f01a414'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_NAME,
        value     => 'Illumina Controls'},
       {attribute => $STUDY_ACCESSION_NUMBER,  value => 'ERP000251'},
       {attribute => $STUDY_ID,                value => '198'},
       {attribute => $STUDY_ID,                value => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra},
       {attribute => $TAG_INDEX,               value => '0'},
       {attribute => $TARGET,                  value => '0'}, # target 0
       {attribute => $TOTAL_READS,             value => '10000'}];

    my $spiked_control = 1;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run7915_lane5_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_198',
                                                       'ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag1_no_spike_bact : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":7915,"position":5,"tag_index":1}]}';
    my $tag1_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":7915,"position":5,"tag_index":1}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '7915'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '5'},
       {attribute => $LIBRARY_ID,               value => '4957423'},
       {attribute => $LIBRARY_TYPE,             value => 'No PCR'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => '619s040'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'ERS012323'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,                value => '230889'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '153.0'},
       {attribute => $SAMPLE_UUID,              value => 'a8b4ebaa-c628-11df-8e7f-00144f2062b0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP000251'},
       {attribute => $STUDY_ID,                 value => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '1'},
       {attribute => $TARGET,                   value => '1'},
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 0;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run7915_lane5_tag1,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag1_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag1_spike_bact : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":7915,"position":5,"tag_index":1}]}';
    my $tag1_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":7915,"position":5,"tag_index":1}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '7915'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '5'},
       {attribute => $LIBRARY_ID,               value => '4957423'},
       {attribute => $LIBRARY_TYPE,             value => 'No PCR'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => '619s040'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'ERS012323'},
       {attribute => $SAMPLE_COMMON_NAME,
        value     => 'Burkholderia pseudomallei'},
       {attribute => $SAMPLE_ID,                value => '230889'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => '153.0'},
							{attribute => $SAMPLE_UUID,              value => 'a8b4ebaa-c628-11df-8e7f-00144f2062b0'},
       {attribute => $STUDY_NAME,
        value     => 'Burkholderia pseudomallei diversity'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP000251'},
       {attribute => $STUDY_ID,                 value => '619'},
       {attribute => $STUDY_TITLE,
        value     => 'Burkholderia pseudomallei diversity' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '1'},
       {attribute => $TARGET,                   value => '1'},
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 1;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run7915_lane5_tag1,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag1_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_619']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_no_spike_human : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":15440,"position":1,"tag_index":0}]}';
    my $tag0_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":15440,"position":1,"tag_index":0}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '15440'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '1'},
       {attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_ID,               value => '12789802'},
       {attribute => $LIBRARY_ID,               value => '12789814'},
       {attribute => $LIBRARY_ID,               value => '12789826'},
       {attribute => $LIBRARY_TYPE,             value => 'Standard'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759045'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759048'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759062'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455639'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455641'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455643'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455647'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759045'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759048'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759062'},
       {attribute => $SAMPLE_ID,                value => '1877285'},
       {attribute => $SAMPLE_ID,                value => '1877289'},
       {attribute => $SAMPLE_ID,                value => '1877292'},
       {attribute => $SAMPLE_ID,                value => '1877306'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '6AJ182'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8AJ1'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8R163'},
							{attribute => $SAMPLE_UUID,              value => 'd41d4a40-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd492a150-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd4ee2ed0-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd62d8ca0-a521-11e3-8055-3c4a9275d6c6'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'EGAS00001002084'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '0'},
       {attribute => $TARGET,                   value => '0'}, # target 0
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 0;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run15440_lane1_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag0_spike_human : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":15440,"position":1,"tag_index":0}]}';
    my $tag0_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":15440,"position":1,"tag_index":0}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '15440'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '1'},
       {attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_ID,               value => '12789802'},
       {attribute => $LIBRARY_ID,               value => '12789814'},
       {attribute => $LIBRARY_ID,               value => '12789826'},
       {attribute => $LIBRARY_ID,               value => '6759268'},
       {attribute => $LIBRARY_TYPE,             value => 'Standard'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759045'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759048'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759062'},
       {attribute => $SAMPLE_NAME,
        value     => 'phiX_for_spiked_buffers'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455639'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455641'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455643'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455647'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759045'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759048'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759062'},
       {attribute => $SAMPLE_ID,                value => '1255141'}, # spike
       {attribute => $SAMPLE_ID,                value => '1877285'},
       {attribute => $SAMPLE_ID,                value => '1877289'},
       {attribute => $SAMPLE_ID,                value => '1877292'},
       {attribute => $SAMPLE_ID,                value => '1877306'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '6AJ182'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8AJ1'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '8R163'},
							{attribute => $SAMPLE_UUID,              value => 'd3a59c4c-c037-11e0-834c-00144f01a414'},
							{attribute => $SAMPLE_UUID,              value => 'd41d4a40-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd492a150-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd4ee2ed0-a521-11e3-8055-3c4a9275d6c6'},
							{attribute => $SAMPLE_UUID,              value => 'd62d8ca0-a521-11e3-8055-3c4a9275d6c6'},
       {attribute => $STUDY_NAME,               value => 'Illumina Controls'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'EGAS00001002084'},
       {attribute => $STUDY_ID,                 value => '198'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '0'},
       {attribute => $TARGET,                   value => '0'}, # target 0
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 1;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run15440_lane1_tag0,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag0_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_198',
                                                       'ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag81_no_spike_human : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":15440,"position":1,"tag_index":81}]}';
    my $tag81_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":15440,"position":1,"tag_index":81}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '15440'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '1'},
       {attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_TYPE,             value => 'Standard'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455639'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_ID,                value => '1877285'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
							{attribute => $SAMPLE_UUID,              value => 'd41d4a40-a521-11e3-8055-3c4a9275d6c6'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'EGAS00001002084'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '81'},
       {attribute => $TARGET,                   value => '1'},
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 0;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run15440_lane1_tag81,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag81_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub update_secondary_metadata_tag81_spike_human : Test(12) {
 SKIP: {
    if (not $samtools_available) {
      skip 'samtools executable not on the PATH', 12;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      group_prefix         => $group_prefix,
                                      group_filter         => $group_filter,
                                      strict_baton_version => 0);

    my $c_json = '{"components":[{"id_run":15440,"position":1,"tag_index":81}]}';
    my $tag81_expected_meta =
      [{attribute => $ALIGNMENT,                value => '1'},
       {attribute => $COMPONENT,                value =>
        '{"id_run":15440,"position":1,"tag_index":81}'},
       {attribute => $COMPOSITION,              value => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $ID_RUN,                   value => '15440'},
       {attribute => $IS_PAIRED_READ,           value => '1'},
       {attribute => $POSITION,                 value => '1'},
       {attribute => $LIBRARY_ID,               value => '12789790'},
       {attribute => $LIBRARY_TYPE,             value => 'Standard'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => 'T19PG5759041'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'EGAN00001455639'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Homo Sapien'},
       {attribute => $SAMPLE_DONOR_ID,          value => 'T19PG5759041'},
       {attribute => $SAMPLE_ID,                value => '1877285'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => '7R5'},
							{attribute => $SAMPLE_UUID,              value => 'd41d4a40-a521-11e3-8055-3c4a9275d6c6'},
       {attribute => $STUDY_NAME,
        value     => 'SEQCAP_Lebanon_LowCov-seq'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'EGAS00001002084'},
       {attribute => $STUDY_ID,                 value => '2967'},
       {attribute => $STUDY_TITLE,
        value     => 'Lebanon_LowCov-seq' . $utf8_extra},
       {attribute => $TAG_INDEX,                value => '81'},
       {attribute => $TARGET,                   value => '1'},
       {attribute => $TOTAL_READS,              value => '10000'}];

    my $spiked_control = 1;

    foreach my $format (qw[bam cram]) {
      # 2 * 4 tests
      test_metadata_update($irods, $lims_factory, $irods_tmp_coll,
                           {data_file              => $run15440_lane1_tag81,
                            format                 => $format,
                            spiked_control         => $spiked_control,
                            expected_metadata      => $tag81_expected_meta,
                            expected_groups_before => [$public_group,
                                                       'ss_10',
                                                       'ss_100'],
                            expected_groups_after  => ['ss_2967']});
    }
  } # SKIP samtools
}

sub test_metadata_update {
  my ($irods, $lims_factory, $working_coll, $args) = @_;

  ref $args eq 'HASH' or croak "The arguments must be a HashRef";

  my $data_file      = $args->{data_file};
  my $initargs       = $args->{initargs};
  my $format         = $args->{format};
  my $spiked         = $args->{spiked_control};
  my $exp_metadata   = $args->{expected_metadata};
  my $exp_grp_before = $args->{expected_groups_before};
  my $exp_grp_after  = $args->{expected_groups_after};

  my $file_name = "$data_file.$format";
  my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
    (collection  => $working_coll,
     data_object => $file_name,
     irods       => $irods,
     @{$initargs});
  my $tag = $obj->tag_index;

  my @secondary_avus = TestAnnotator->new->make_secondary_metadata
    ($obj->composition, $lims_factory,
     with_spiked_control => $spiked);

  my @groups_before = $obj->get_groups;

  my %secondary_attrs = map { $_->{attribute} => 1 } @secondary_avus;
  my $expected_num_attrs = scalar keys %secondary_attrs;
  my ($num_attributes, $num_processed, $num_errors) =
    $obj->update_secondary_metadata(@secondary_avus);
  cmp_ok($num_attributes, '==', $expected_num_attrs,
         "Secondary metadata attrs; $format, tag: $tag, spiked: $spiked");
  cmp_ok($num_processed, '==', $expected_num_attrs,
         "Secondary metadata processed; $format, tag: $tag, spiked: $spiked");
  cmp_ok($num_errors, '==', 0,
         "Secondary metadata errors; $format, tag: $tag, spiked: $spiked");

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

sub _build_initargs {
  my ($file_composition, $key_path) = @_;

  my ($id_run, $position, $tag_index, $subset) =
    @{$file_composition->{$key_path}};
  my @initargs  = (id_run    => $id_run,
                   position  => $position,
                   tag_index => $tag_index);
  push @initargs, subset => $subset if defined $subset;

  return @initargs;
}

sub _composition_json2product_id {
  my $c_json = shift;
  my $c_class = 'npg_tracking::glossary::composition::component::illumina';
  return npg_tracking::glossary::composition->thaw(
    $c_json, component_class => $c_class)->digest;
}

1;
