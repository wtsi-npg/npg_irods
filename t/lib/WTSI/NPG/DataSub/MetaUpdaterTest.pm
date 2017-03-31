package WTSI::NPG::DataSub::MetaUpdaterTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use DBI;
use Test::MockObject::Extends;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS::Publisher;
use WTSI::NPG::DataSub::MetaUpdater;
use WTSI::NPG::DataSub::SubtrackClient;

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/datasub';

my $irods_tmp_coll;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("DatasubMetaUpdaterTest.$pid.$test_counter");
  $test_counter++;

  my $publisher = WTSI::NPG::iRODS::Publisher->new(irods => $irods);

  # The files with distinct name and MD5
  $irods->add_collection("$irods_tmp_coll/valid");
  foreach my $file (qw[a.txt b.txt c.txt d.txt]) {
    $publisher->publish_file("$data_path/valid/$file",
                             "$irods_tmp_coll/valid");
  }

  # Two files with the same name and MD5
  $irods->add_collection("$irods_tmp_coll/invalid/x");
  $publisher->publish_file("$data_path/invalid/a.txt",
                           "$irods_tmp_coll/invalid");
  $publisher->publish_file("$data_path/invalid/x/a.txt",
                           "$irods_tmp_coll/invalid/x");
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub update_submission_metadata : Test(22) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $meta_updater = WTSI::NPG::DataSub::MetaUpdater->new(irods => $irods);

  # Dummy subtrack database data for 3 files with distinct name/MD5
  # pairs
  my @expected_valid = ({
                         ebi_run_acc => "ERR1609600",
                         ebi_sub_acc => "ERA697039",
                         file_name   => "a.txt",
                         md5         => "a9fdbcfbce13a3d8dee559f58122a31c",
                         timestamp   => "2016-09-09"
                        },
                        {
                         ebi_run_acc => "ERR1609601",
                         ebi_sub_acc => "ERA697039",
                         file_name   => "b.txt",
                         md5         => "76cf56576e1455207b6d972d2de9c31a",
                         timestamp   => "2016-09-09"
                        },
                        {
                         ebi_run_acc => "ERR1609602",
                         ebi_sub_acc => "ERA697039",
                         file_name   => "c.txt",
                         md5         => "aa5cd13d095084968efe1e2fc4fc1827",
                         timestamp   => "2016-09-09"
                        });

  my $dbh_valid = _make_mock_dbh(@expected_valid);
  my @files_valid = WTSI::NPG::DataSub::SubtrackClient->new
    (dbh => $dbh_valid)->query_submitted_files;

  # These 3 files should have submission metadata added
  cmp_ok($meta_updater->update_submission_metadata("$irods_tmp_coll/valid",
                                                   \@files_valid),
         '==', 3, "Updated metadata on unambiguous name/MD5 results");

  foreach my $item (@expected_valid) {
    my $path = "$irods_tmp_coll/valid/" . $item->{file_name};
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);

    is($obj->get_avu('ebi_run_acc')->{value}, $item->{ebi_run_acc},
       'EBI run accession AVU') or diag explain $obj->metadata;

    is($obj->get_avu('ebi_sub_acc')->{value}, $item->{ebi_sub_acc},
       'EBI submission accession AVU') or diag explain $obj->metadata;

    is($obj->get_avu('ebi_sub_date')->{value}, $item->{timestamp},
       'EBI submission date AVU') or diag explain $obj->metadata;

    is($obj->get_avu('ebi_sub_md5')->{value}, $item->{md5},
       'EBI submission MD5 AVU') or diag explain $obj->metadata;
  }

  # Dummy subtrack database data for 2 files with the same name/MD5
  # pair
  my @expected_invalid = ({
                           ebi_run_acc => "ERR1609600",
                           ebi_sub_acc => "ERA697039",
                           file_name   => "a.txt",
                           md5         => "a9fdbcfbce13a3d8dee559f58122a31c",
                           timestamp   => "2016-09-09"
                          });

  my $dbh_invalid = _make_mock_dbh(@expected_invalid);
  my @files_invalid = WTSI::NPG::DataSub::SubtrackClient->new
    (dbh => $dbh_invalid)->query_submitted_files;

  # These files should not have submission metadata added because they
  # are duplicates
  cmp_ok($meta_updater->update_submission_metadata("$irods_tmp_coll/invalid",
                                                   \@files_invalid),
         '==', 0, "No metadata update on ambiguous name/MD5 results");

  foreach my $coll ("$irods_tmp_coll/invalid/", "$irods_tmp_coll/invalid/x/") {
    my $path = "$coll/a.txt";
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);

    foreach my $attr (qw[ebi_run_acc ebi_sub_acc ebi_sub_date ebi_sub_md5]) {
      is($obj->get_avu($attr), undef, "$attr absent from ambiguous path");
    }
  }
}

sub _make_mock_dbh {
  my (@expected) = @_;

  my $sth = Test::MockObject->new;
  $sth->set_true('execute');
  $sth->set_series('fetchrow_hashref', @expected);

  my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', q[], q[]);
  $dbh = Test::MockObject::Extends->new($dbh);
  $dbh->set_always('prepare', $sth);

  return $dbh;
}

1;
