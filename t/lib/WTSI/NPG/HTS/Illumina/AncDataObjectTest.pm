package WTSI::NPG::HTS::Illumina::AncDataObjectTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions;
use File::Temp;
use List::AllUtils qw[any];
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::Illumina::AncDataObject;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

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
my $data_path    = './t/data/anc_data_object';
my $fixture_path = "./t/fixtures";

my $db_dir = File::Temp->newdir;
my $wh_schema;
my $lims_factory;

my $irods_tmp_coll;

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
my @irods_groups = map { $group_prefix . $_ }
  (10, 100, 198, 619, 2967, 3291, 3720);
push @irods_groups, $public_group;

# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

my %file_composition =
  ('17550_3#1.bamcheck'                          => [17550, 3, 1,  undef],
   '17550_3#1.deletions.bed'                     => [17550, 3, 1,  undef],
   '17550_3#1.insertions.bed'                    => [17550, 3, 1,  undef],
   '17550_3#1.junctions.bed'                     => [17550, 3, 1,  undef],
   '17550_3#1.seqchksum'                         => [17550, 3, 1,  undef],
   '17550_3#1.sha512primesums512.seqchksum'      => [17550, 3, 1,  undef],
   '17550_3#1_F0x900.stats'                      => [17550, 3, 1,  undef],
   '17550_3#1_F0xB00.stats'                      => [17550, 3, 1,  undef],
   '17550_3#1_phix.bam_flagstats.json'           => [17550, 3, 1, 'phix'],
   '17550_3#1_phix.bamcheck'                     => [17550, 3, 1, 'phix'],
   '17550_3#1_phix.flagstat'                     => [17550, 3, 1, 'phix'],
   '17550_3#1_phix.seqchksum'                    => [17550, 3, 1, 'phix'],
   '17550_3#1_phix.sha512primesums512.seqchksum' => [17550, 3, 1, 'phix'],
   '17550_3#1_phix_F0x900.stats'                 => [17550, 3, 1, 'phix'],
   '17550_3#1_phix_F0xB00.stats'                 => [17550, 3, 1, 'phix'],
   '17550_3#1_phix_quality_cycle_caltable.txt'   => [17550, 3, 1, 'phix'],
   '17550_3#1_phix_quality_cycle_surv.txt'       => [17550, 3, 1, 'phix'],
   '17550_3#1_phix_quality_error.txt'            => [17550, 3, 1, 'phix'],
   '17550_3#1_quality_cycle_caltable.txt'        => [17550, 3, 1, undef],
   '17550_3#1_quality_cycle_surv.txt'            => [17550, 3, 1, undef],
   '17550_3#1_quality_error.txt'                 => [17550, 3, 1, undef],
   '17550_3#1.junctions.tab'                     => [17550, 3, 1, undef],
   '17550_3#1.readspergene.tab'                  => [17550, 3, 1, undef],
  );

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
    $irods->add_collection("Illumina::AncDataObjectTest.$pid.$test_counter");
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

  $irods->put_collection($data_path, $irods_tmp_coll);

  foreach my $data_file (sort keys %file_composition) {
    my $full_path = "$irods_tmp_coll/anc_data_object/$data_file";
    my @initargs = _build_initargs(\%file_composition, $data_file);
    my $obj = WTSI::NPG::HTS::Illumina::AncDataObject->new
      ($irods, $full_path, @initargs);

    if ($group_tests_enabled) {
      # Add some test group permissions
      $irods->set_object_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                     $public_group, $full_path);
      foreach my $group (map { $group_prefix . $_ } (10, 100)) {
        $irods->set_object_permissions
          ($WTSI::NPG::iRODS::READ_PERMISSION, $group, $full_path);
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
  require_ok('WTSI::NPG::HTS::Illumina::AncDataObject');
}

sub update_secondary_metadata_no_spike_human : Test(138) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    group_prefix         => $group_prefix,
                                    group_filter         => $group_filter,
                                    strict_baton_version => 0);
  my $spiked_control = 0;

  foreach my $data_file (sort keys %file_composition) {
    my ($name, $path, $suffix) = fileparse($data_file, '.bed', '.json', '.tab');

    my @expected_groups_before = ($public_group, 'ss_10', 'ss_100');
    my @expected_groups_after;

    my @expected_metadata;
    if (any { $suffix eq $_ } ('.bed', '.json', '.tab')) {
      push @expected_metadata, {attribute => $STUDY_ID, value => '3291'};
      @expected_groups_after = ('ss_3291');
    }
    else {
      @expected_groups_after = @expected_groups_before;
    }

    test_metadata_update($irods, $lims_factory,
                         "$irods_tmp_coll/anc_data_object",
                         {data_file              => $data_file,
                          spiked_control         => $spiked_control,
                          expected_metadata      => \@expected_metadata,
                          expected_groups_before => \@expected_groups_before,
                          expected_groups_after  => \@expected_groups_after});
  }
}

sub test_metadata_update {
  my ($irods, $lims_factory, $working_coll, $args) = @_;

  ref $args eq 'HASH' or croak "The arguments must be a HashRef";

  my $data_file      = $args->{data_file};
  my $spiked         = $args->{spiked_control};
  my $exp_metadata   = $args->{expected_metadata};
  my $exp_grp_before = $args->{expected_groups_before};
  my $exp_grp_after  = $args->{expected_groups_after};

  my @initargs = _build_initargs(\%file_composition, $data_file);

  my $obj = WTSI::NPG::HTS::Illumina::AncDataObject->new
    (collection  => $working_coll,
     data_object => $data_file,
     irods       => $irods, @initargs);
  my $tag = $obj->tag_index;

  my $lims = $lims_factory->make_lims($obj->composition);
  my @secondary_avus =
    TestAnnotator->new->make_study_id_metadata($lims, $spiked);

  my @groups_before = $obj->get_groups;

  my $expected_num_attrs = 0;
  if ($obj->is_restricted_access) {
    $expected_num_attrs = 1; # study_id
  }

  my ($num_attributes, $num_processed, $num_errors) =
    $obj->update_secondary_metadata(@secondary_avus);
  cmp_ok($num_attributes, '==', $expected_num_attrs,
         "Secondary metadata attrs; $data_file, " .
         "tag: $tag, spiked: $spiked");
  cmp_ok($num_processed, '==', $expected_num_attrs,
         "Secondary metadata processed; $data_file, " .
         "tag: $tag, spiked: $spiked");
  cmp_ok($num_errors, '==', 0,
         "Secondary metadata errors; $data_file, " .
         "tag: $tag, spiked: $spiked");

  my @groups_after = $obj->get_groups;

  my $metadata = $obj->metadata;
  is_deeply($metadata, $exp_metadata,
            "Secondary metadata was updated; $data_file, " .
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
  my ($test_paths, $key_path) = @_;

  my ($id_run, $position, $tag_index, $subset) = @{$test_paths->{$key_path}};
  my @initargs  = (id_run    => $id_run,
                   position  => $position,
                   tag_index => $tag_index);
  push @initargs, subset => $subset if defined $subset;

  return @initargs;
}

1;
