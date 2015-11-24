package WTSI::NPG::HTS::PublisherTest;

use strict;
use warnings;

use Data::Dump qw(pp);
use English qw(-no_match_vars);
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;
use URI;

use base qw(WTSI::NPG::HTS::Test);

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::HTS::Publisher;

my $pid = $PID;
my $fixture_counter = 0;

my $data_path = './t/data/publisher';
my $cwc;
my $irods_tmp_coll;


sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $cwc = $irods->working_collection;

  $irods_tmp_coll = $irods->add_collection
    ("PublisherTest.$pid.$fixture_counter");
  $fixture_counter++;
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::Publisher');
}

sub publish_file : Test(33) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # publish_file with new full path, no metadata, no timestamp
  pub_new_full_path_no_meta_no_stamp($irods, $data_path, $irods_tmp_coll);
  # publish_file with new full path, some metadata, no timestamp
  pub_new_full_path_meta_no_stamp($irods, $data_path, $irods_tmp_coll);
  # publish_file with new full path, no metadata, with timestamp
  pub_new_full_path_no_meta_stamp($irods, $data_path, $irods_tmp_coll);

  # publish_file with existing full path, no metadata, no timestamp,
  # matching MD5
  pub_exist_full_path_no_meta_no_stamp_match($irods, $data_path,
                                             $irods_tmp_coll);
  # publish_file with existing full path, some metadata, no timestamp,
  # matching MD5
  pub_exist_full_path_meta_no_stamp_match($irods, $data_path,
                                          $irods_tmp_coll);

  # publish_file with existing full path, no metadata, no timestamp,
  # non-matching MD5
  pub_exist_full_path_no_meta_no_stamp_no_match($irods, $data_path,
                                                $irods_tmp_coll);
  # publish_file with existing full path, some metadata, no timestamp,
  # non-matching MD5
  pub_exist_full_path_meta_no_stamp_no_match($irods, $data_path,
                                             $irods_tmp_coll);
}

sub pub_new_full_path_no_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, no metadata, no timestamp
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pub_new_full_path_no_meta_no_stamp.txt";
  is($publisher->publish_file($local_path_a, $remote_path),
     $remote_path,
     'publish_file, full path, no additional metadata, default timestamp');

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  like($obj->get_avu($DCTERMS_CREATED)->{value}, qr{^$timestamp_regex$},
       'New object default creation timestamp') or diag explain $obj->metadata;

  is($obj->get_avu($DCTERMS_CREATOR)->{value}, 'http://www.sanger.ac.uk',
     'New object creator URI') or diag explain $obj->metadata;

  ok(URI->new($obj->get_avu($DCTERMS_PUBLISHER)->{value}), 'Publisher URI') or
    diag explain $obj->metadata;

  is($obj->get_avu($FILE_MD5)->{value}, 'a9fdbcfbce13a3d8dee559f58122a31c',
     'New object MD5') or diag explain $obj->metadata;
}

sub pub_new_full_path_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, some metadata, no timestamp
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pub_new_full_path_meta_no_stamp.txt";
  my $additional_avu1 = $irods->make_avu($RT_TICKET, '1234567890');
  my $additional_avu2 = $irods->make_avu($ANALYSIS_UUID,
                                         'abcdefg-01234567890-wxyz');

  is($publisher->publish_file($local_path_a, $remote_path,
                              [$additional_avu1, $additional_avu2]),
     $remote_path,
     'publish_file, full path, additional metadata, default timestamp');

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);

  is($obj->get_avu($RT_TICKET)->{value}, $additional_avu1->{value},
     'New additional AVU 1') or diag explain $obj->metadata;

  is($obj->get_avu($ANALYSIS_UUID)->{value}, $additional_avu2->{value},
     'New additional AVU 2') or diag explain $obj->metadata;
}

sub pub_new_full_path_no_meta_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, no metadata, no timestamp
  my $timestamp = DateTime->now;
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pub_new_full_path_no_meta_stamp.txt";

  is($publisher->publish_file($local_path_a, $remote_path, [], $timestamp),
     $remote_path,
     'publish_file, full path, no additional metadata, supplied timestamp');

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  is($obj->get_avu($DCTERMS_CREATED)->{value}, $timestamp->iso8601,
     'New object supplied creation timestamp') or diag explain $obj->metadata;

  is($obj->get_avu($DCTERMS_CREATOR)->{value}, 'http://www.sanger.ac.uk',
     'New object creator URI') or diag explain $obj->metadata;

  ok(URI->new($obj->get_avu($DCTERMS_PUBLISHER)->{value}), 'Publisher URI') or
    diag explain $obj->metadata;

  is($obj->get_avu($FILE_MD5)->{value}, 'a9fdbcfbce13a3d8dee559f58122a31c',
     'New object MD5') or diag explain $obj->metadata;
}

sub pub_exist_full_path_no_meta_no_stamp_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, no metadata, no timestamp,
  # matching MD5
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pub_exist_full_path_no_meta_no_stamp_match.txt";
  $publisher->publish_file($local_path_a, $remote_path) or fail;

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification timestamp before');

  is($publisher->publish_file($local_path_a, $remote_path),
     $remote_path,
     'publish_file, existing full path, MD5 match');

  $obj->clear_metadata;
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification AVU after') or
    diag explain $obj->metadata;
}

sub pub_exist_full_path_meta_no_stamp_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, some metadata, no timestamp,
  # matching MD5
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pub_exist_full_path_meta_no_stamp_match.txt";
  my $additional_avu1 = $irods->make_avu($RT_TICKET, '1234567890');
  my $additional_avu2 = $irods->make_avu($ANALYSIS_UUID,
                                         'abcdefg-01234567890-wxyz');
  $publisher->publish_file($local_path_a, $remote_path) or fail;

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification timestamp before');
  ok(!$obj->get_avu($RT_TICKET), 'No additional AVU 1 before');
  ok(!$obj->get_avu($ANALYSIS_UUID), 'No additional AVU 2 before');

  is($publisher->publish_file($local_path_a, $remote_path,
                              [$additional_avu1, $additional_avu2]),
     $remote_path,
     'publish_file, existing full path, MD5 match');

  $obj->clear_metadata;
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification AVU after') or
    diag explain $obj->metadata;

  is($obj->get_avu($RT_TICKET)->{value}, $additional_avu1->{value},
     'New additional AVU 1') or diag explain $obj->metadata;

  is($obj->get_avu($ANALYSIS_UUID)->{value}, $additional_avu2->{value},
     'New additional AVU 2') or diag explain $obj->metadata;
}

sub pub_exist_full_path_no_meta_no_stamp_no_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, no metadata, no timestamp,
  # non-matching MD5
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path =
    "$irods_tmp_coll/pub_exist_full_path_no_meta_no_stamp_no_match";
  $publisher->publish_file($local_path_a, $remote_path) or fail;
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification timestamp before');

  my $local_path_b = "$data_path/publish_file/b.txt";
  is($publisher->publish_file($local_path_b, $remote_path),
     $remote_path,
     'publish_file, existing full path, MD5 non-match');

  $obj->clear_metadata;
  like($obj->get_avu($DCTERMS_MODIFIED)->{value},qr{^$timestamp_regex$},
       'Modification AVU present after') or diag explain $obj->metadata;
}
sub pub_exist_full_path_meta_no_stamp_no_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, some metadata, no timestamp,
  # non-matching MD5
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path =
    "$irods_tmp_coll/pub_exist_full_path_meta_no_stamp_no_match.txt";
  my $additional_avu1 = $irods->make_avu($RT_TICKET, '1234567890');
  my $additional_avu2 = $irods->make_avu($ANALYSIS_UUID,
                                         'abcdefg-01234567890-wxyz');
  $publisher->publish_file($local_path_a, $remote_path) or fail;
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
  ok(!$obj->get_avu($DCTERMS_MODIFIED), 'No modification timestamp before');
  ok(!$obj->get_avu($RT_TICKET), 'No additional AVU 1 before');
  ok(!$obj->get_avu($ANALYSIS_UUID), 'No additional AVU 2 before');

  my $local_path_b = "$data_path/publish_file/b.txt";
  is($publisher->publish_file($local_path_b, $remote_path,
                              [$additional_avu1, $additional_avu2]),
     $remote_path,
     'publish_file, existing full path, MD5 non-match');

  $obj->clear_metadata;
  like($obj->get_avu($DCTERMS_MODIFIED)->{value}, qr{^$timestamp_regex$},
       'Modification AVU present after') or diag explain $obj->metadata;

  is($obj->get_avu($RT_TICKET)->{value}, $additional_avu1->{value},
     'New additional AVU 1') or diag explain $obj->metadata;

  is($obj->get_avu($ANALYSIS_UUID)->{value}, $additional_avu2->{value},
     'New additional AVU 2') or diag explain $obj->metadata;
}

1;
