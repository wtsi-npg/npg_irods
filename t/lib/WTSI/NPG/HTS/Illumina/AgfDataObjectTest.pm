package WTSI::NPG::HTS::Illumina::AgfDataObjectTest;

use utf8;

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

use WTSI::NPG::HTS::Illumina::AgfDataObject;
use WTSI::NPG::HTS::LIMSFactory;
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
my $data_path    = './t/data/agf_data_object';
my $fixture_path = "./t/fixtures";

my $utf8_extra = '[UTF-8 test: Τὴ γλῶσσα μοῦ ἔδωσαν ἑλληνικὴ το σπίτι φτωχικό στις αμμουδιές του Ομήρου.]';

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
  (10, 100, 198, 619, 2905, 2967, 3291, 3720);
push @irods_groups, $public_group;

# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

my $run17550_lane8      = '17550_8';
my $run17550_lane8_tag1 = '17550_8#1';

my %file_composition = ('17550_8'   => [17550, 8, undef],
                        '17550_8#1' => [17550, 8, 1]);

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
    $irods->add_collection("Illumina::AgfDataObjectTest.$pid.$test_counter");
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

  foreach my $format (qw[vcf geno]) {
    my $full_path =
      "$irods_tmp_coll/agf_data_object/$run17550_lane8_tag1.$format";

    my @initargs = _build_initargs(\%file_composition, $run17550_lane8_tag1);
    my $obj = WTSI::NPG::HTS::Illumina::AgfDataObject->new
      ($irods, $full_path, @initargs);

    my @avus;
    push @avus, TestAnnotator->new->make_composition_metadata
      ($obj->composition);

    foreach my $avu (@avus) {
      my $attribute = $avu->{attribute};
      my $value     = $avu->{value};
      my $units     = $avu->{units};
      $obj->supersede_avus($attribute, $value, $units);
    }

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
  require_ok('WTSI::NPG::HTS::Illumina::AgfDataObject');
}

sub is_restricted_access : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # Without any study metadata information
  foreach my $format (qw[geno vcf]) {
    foreach my $path (sort keys %file_composition) {
      my $full_path = "/seq/17550/$path.$format";
      my @initargs = _build_initargs(\%file_composition, $path);

      my $obj = WTSI::NPG::HTS::Illumina::AgfDataObject->new
        ($irods, $full_path, @initargs);
      ok($obj->is_restricted_access, "$full_path is restricted_access");
    }
  }
}

sub update_secondary_metadata_tag1_no_spike_human : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    group_prefix         => $group_prefix,
                                    group_filter         => $group_filter,
                                    strict_baton_version => 0);

  my $spiked_control = 0;
  foreach my $format (qw[geno vcf]) {
    my $data_file = "$run17550_lane8_tag1.$format";

    my @expected_groups_before = ($public_group, 'ss_10', 'ss_100');
    my @expected_groups_after  = ('ss_2905');

    my $c_json = '{"components":[{"id_run":17550,"position":8,"tag_index":1}]}';
    my $expected_metadata =
      [{attribute => $COMPONENT,
        value     => '{"id_run":17550,"position":8,"tag_index":1}'},
       {attribute => $COMPOSITION,              value     => $c_json},
       {attribute => $ID_PRODUCT,
        value => _composition_json2product_id($c_json)},
       {attribute => $LIBRARY_ID,               value => '14727840'},
       {attribute => $LIBRARY_TYPE,             value => 'ChIP-Seq Auto'},
       {attribute => $QC_STATE,                 value => '1'},
       {attribute => $SAMPLE_NAME,              value => '2905STDY6178180'},
       {attribute => $SAMPLE_ACCESSION_NUMBER,  value => 'ERS811018'},
       {attribute => $SAMPLE_COMMON_NAME,       value => 'Mus musculus'},
       {attribute => $SAMPLE_DONOR_ID,          value => '2905STDY6178180'},
       {attribute => $SAMPLE_ID,                value => '2376982'},
       {attribute => $SAMPLE_PUBLIC_NAME,       value => 'UTX_IP_UTX_GTX_A'},
       {attribute => $SAMPLE_SUPPLIER_NAME,     value => 'UTX_IP_UTX_GTX_A'},
       {attribute => $STUDY_NAME,
        value     => 'Analysis of the chromatin state of mouse stem and progenitor cell compartment'},
       {attribute => $STUDY_ACCESSION_NUMBER,   value => 'ERP004563'},
       {attribute => $STUDY_ID,                 value => '2905'},
       {attribute => $STUDY_TITLE,
        value     => 'Analysis of the chromatin state of mouse stem and progenitor cell compartment      '. $utf8_extra},];

    test_metadata_update($irods, $lims_factory,
                         "$irods_tmp_coll/agf_data_object",
                         {data_file              => $data_file,
                          spiked_control         => $spiked_control,
                          expected_metadata      => $expected_metadata,
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

  my $obj = WTSI::NPG::HTS::Illumina::AgfDataObject->new
    (collection  => $working_coll,
     data_object => $data_file,
     irods       => $irods);
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

sub _composition_json2product_id {
  my $c_json = shift;
  my $c_class = 'npg_tracking::glossary::composition::component::illumina';
  return npg_tracking::glossary::composition->thaw(
    $c_json, component_class => $c_class)->digest;
}

1;
