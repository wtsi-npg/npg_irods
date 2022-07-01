package WTSI::NPG::Data::SingleReplicaMetaUpdaterTest;

use strict;
use warnings;

use Cwd qw[cwd];
use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel catfile];
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::Data::SingleReplicaMetaUpdater;
use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = cwd . q[/] . 't/data/single_replica';

my $irods_tmp_coll;

my $two_years = DateTime::Duration->new(years => 2);
my $one_day   = DateTime::Duration->new(days => 1);

my @datetime_args = (year      => 2022,
                     month     => 1,
                     day       => 1,
                     hour      => 9,
                     minute    => 15,
                     second    => 0,
                     time_zone => 'Europe/London');
my $early  = DateTime->new(@datetime_args)->subtract($two_years);
my $middle = DateTime->new(@datetime_args);
my $recent = DateTime->new(@datetime_args)->add($one_day);

sub setup_fixture : Test(startup) {
  WTSI::DNAP::Utilities::Runnable->new
    (executable => './scripts/add_single_replica_query.sh')->run;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("SingleReplicaMetaUpdaterTest.$pid.$test_counter");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my @objs;
  foreach my $i (1 .. 10) {
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods,
                                                "$irods_tmp_coll/single_replica/$i.txt");
    $obj->calculate_checksum;
    $obj->add_avu('md5', $obj->checksum);
    $obj->add_avu('ebi_sub_md5', $obj->checksum);
    push @objs, $obj;
  }

  # 5 objects older than 2 year retention grace period
  foreach my $obj (@objs[0 .. 4]) {
    $obj->add_avu('dcterms:created', $early->iso8601);
  }
  # 2 objects objects exactly on the 2 year retention grace period
  foreach my $obj (@objs[5 .. 6]) {
    $obj->add_avu('dcterms:created', $middle->iso8601);
  }
  # 3 objects within the 2 year retention grace period
  foreach my $obj (@objs[7 .. 9]) {
    $obj->add_avu('dcterms:created', $recent->iso8601);
  }

  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub find_candidate_objects : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $m = WTSI::NPG::Data::SingleReplicaMetaUpdater->new(irods => $irods);

  # The first 5 objects are older than the threshold (middle) time point (the
  # others are exactly on it, or more recent.
  my ($num_objs, $num_processed, $num_errors) =
    $m->update_single_replica_metadata(end_date => $middle);
  is($num_objs, 5, 'Expected 5 objects found');
  is($num_processed, 5, 'Expected 5 objects processed');
  is($num_errors, 0, 'Expected no errors');

  my $sr = $WTSI::NPG::Data::SingleReplicaMetaUpdater::SINGLE_REPLICA_ATTR;
  my @expected = map { "$irods_tmp_coll/single_replica/$_.txt" } 1 .. 5;

  my @observed = $irods->find_objects_by_meta($irods_tmp_coll, [$sr => 1]);
  is_deeply(\@observed, \@expected, 'Single replica metadata added') or diag
    explain \@observed;
}

sub avoid_inconsistent_objects : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $m = WTSI::NPG::Data::SingleReplicaMetaUpdater->new(irods => $irods);
  my $ebi_md5 = $WTSI::NPG::Data::SingleReplicaMetaUpdater::EBI_SUB_MD5_ATTR;

  # Make the first of the candidate objects inconsistent by removing its
  # ebi_sub_md5 metadata. This means that the object will not be found.
  my $obj1 =
    WTSI::NPG::iRODS::DataObject->new($irods,
                                      "$irods_tmp_coll/single_replica/1.txt");
  $obj1->remove_avu($ebi_md5, '68b22040025784da775f55cfcb6dee2e');

  # Make the second of the candidate objects inconsistent by changing its
  # ebi_sub_md5 metadata. This means the object will be found, but will
  # raise an error on processing.
  my $obj2 =
    WTSI::NPG::iRODS::DataObject->new($irods,
                                      "$irods_tmp_coll/single_replica/2.txt");
  $obj2->supersede_avus($ebi_md5, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');

  my ($num_objs, $num_processed, $num_errors) =
    $m->update_single_replica_metadata(end_date => $middle);
  is($num_objs, 4, 'Expected 4 objects found');
  is($num_processed, 4, 'Expected 4 objects processed');
  is($num_errors, 1, 'Expected 1 error');

  my $sr = $WTSI::NPG::Data::SingleReplicaMetaUpdater::SINGLE_REPLICA_ATTR;
  my @expected = map { "$irods_tmp_coll/single_replica/$_.txt" } 3 .. 5;

  my @observed = $irods->find_objects_by_meta($irods_tmp_coll, [$sr => 1]);
  is_deeply(\@observed, \@expected, 'Single replica metadata added') or diag
    explain \@observed;
}

sub limit_number_processed_more_found : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $m = WTSI::NPG::Data::SingleReplicaMetaUpdater->new(irods => $irods);

  # We find more candidates than the limit number, so the limit should be in
  # effect.

  # The first 5 objects are older than the threshold (middle) time point (the
  # others are exactly on it, or more recent. We limit that to just 2
  my ($num_objs, $num_processed, $num_errors) =
      $m->update_single_replica_metadata(end_date => $middle,
                                         limit    => 2);
  is($num_objs, 2, 'Expected 2 objects found');
  is($num_processed, 2, 'Expected 2 objects processed');
  is($num_errors, 0, 'Expected no errors');

  my $sr = $WTSI::NPG::Data::SingleReplicaMetaUpdater::SINGLE_REPLICA_ATTR;
  my @expected = map { "$irods_tmp_coll/single_replica/$_.txt" } 1 .. 2;

  my @observed = $irods->find_objects_by_meta($irods_tmp_coll, [$sr => 1]);
  is_deeply(\@observed, \@expected, 'Single replica metadata added') or diag
      explain \@observed;
}

sub limit_number_processed_fewer_found : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
      strict_baton_version => 0);

  my $m = WTSI::NPG::Data::SingleReplicaMetaUpdater->new(irods => $irods);

  # We find fewer candidates than the limit number, so the limit should have
  # no effect.

  # The first 5 objects are older than the threshold (middle) time point (the
  # others are exactly on it, or more recent.
  my ($num_objs, $num_processed, $num_errors) =
      $m->update_single_replica_metadata(end_date => $middle,
                                         limit    => 10);
  is($num_objs, 5, 'Expected 5 objects found');
  is($num_processed, 5, 'Expected 5 objects processed');
  is($num_errors, 0, 'Expected no errors');

  my $sr = $WTSI::NPG::Data::SingleReplicaMetaUpdater::SINGLE_REPLICA_ATTR;
  my @expected = map { "$irods_tmp_coll/single_replica/$_.txt" } 1 .. 5;

  my @observed = $irods->find_objects_by_meta($irods_tmp_coll, [$sr => 1]);
  is_deeply(\@observed, \@expected, 'Single replica metadata added') or diag
      explain \@observed;
}




1;
