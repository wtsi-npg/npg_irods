package WTSI::NPG::HTS::Illumina::LogPublisherTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw[$ID_RUN];
use WTSI::NPG::HTS::Illumina::LogPublisher;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/log_publisher';

my $irods_tmp_coll;

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

sub publish_logs : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $runfolder_path1 = "$data_path/100818_IL32_10371";
  my $pub1 = WTSI::NPG::HTS::Illumina::LogPublisher->new
    (irods           => $irods,
     runfolder_path  => $runfolder_path1,
     id_run          => 999,
     dest_collection => "$irods_tmp_coll/publish_logs_given_id_run");

  my $log_archive1 = $pub1->publish_logs;
  ok($log_archive1, 'Log archive created given an id_run');

  my $obj1 = WTSI::NPG::HTS::DataObject->new($irods, $log_archive1);
  is_deeply($obj1->get_avu($ID_RUN),
            {attribute => $ID_RUN,
             value     => 999}, "$ID_RUN metadata is present");

  # Test inferring of the id_run by using the fake runfolder used to
  # test the RunPublisher
  my $runfolder_path2 =
    "t/data/run_publisher/sequence/150910_HS40_17550_A_C75BCANXX";
  my $pub2 = WTSI::NPG::HTS::Illumina::LogPublisher->new
    (irods           => $irods,
     runfolder_path  => $runfolder_path2,
     dest_collection => "$irods_tmp_coll/publish_logs_inferred_id_run");

  my $log_archive2 = $pub2->publish_logs;
  ok($log_archive2, 'Log archive created given a runfolder path');

  my $obj2 = WTSI::NPG::HTS::DataObject->new($irods, $log_archive2);
  is_deeply($obj2->get_avu($ID_RUN),
            {attribute => $ID_RUN,
             value     => 17550}, "Inferred $ID_RUN metadata is present");
}

1;
