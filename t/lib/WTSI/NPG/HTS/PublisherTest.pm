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

my $test_counter = 0;
my $data_path = './t/data/publisher';

my $cwc;
my $irods_tmp_coll;

my $pid = $PID;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $cwc = $irods->working_collection;

  $irods_tmp_coll = $irods->add_collection("PublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::Publisher');
}

sub publish : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  my $local_file_path = "$data_path/publish/a.txt";
  my $remote_file_path = "$irods_tmp_coll/a.txt";
  is($publisher->publish($local_file_path, $remote_file_path),
     $remote_file_path, 'publish, file');
  ok($irods->is_object($remote_file_path), 'publish, file -> data object');

  my $local_dir_path = "$data_path/publish";
  my $remote_dir_path = $irods_tmp_coll;
  is($publisher->publish($local_dir_path, $remote_dir_path),
     "$remote_dir_path/publish", 'publish, directory');
  ok($irods->is_collection("$remote_dir_path/"),
     'publish, directory -> collection');
}

sub publish_file : Test(36) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # publish_file with new full path, no metadata, no timestamp
  pf_new_full_path_no_meta_no_stamp($irods, $data_path, $irods_tmp_coll);
  # publish_file with new full path, some metadata, no timestamp
  pf_new_full_path_meta_no_stamp($irods, $data_path, $irods_tmp_coll);
  # publish_file with new full path, no metadata, with timestamp
  pf_new_full_path_no_meta_stamp($irods, $data_path, $irods_tmp_coll);

  # publish_file with existing full path, no metadata, no timestamp,
  # matching MD5
  pf_exist_full_path_no_meta_no_stamp_match($irods, $data_path,
                                            $irods_tmp_coll);
  # publish_file with existing full path, some metadata, no timestamp,
  # matching MD5
  pf_exist_full_path_meta_no_stamp_match($irods, $data_path,
                                         $irods_tmp_coll);

  # publish_file with existing full path, no metadata, no timestamp,
  # non-matching MD5
  pf_exist_full_path_no_meta_no_stamp_no_match($irods, $data_path,
                                               $irods_tmp_coll);
  # publish_file with existing full path, some metadata, no timestamp,
  # non-matching MD5
  pf_exist_full_path_meta_no_stamp_no_match($irods, $data_path,
                                            $irods_tmp_coll);
}

sub publish_directory : Test(11) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # publish_directory with new full path, no metadata, no timestamp
  pd_new_full_path_no_meta_no_stamp($irods, $data_path, $irods_tmp_coll);

  # publish_file with new full path, some metadata, no timestamp
  pd_new_full_path_meta_no_stamp($irods, $data_path, $irods_tmp_coll);
}

sub pf_new_full_path_no_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, no metadata, no timestamp
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pf_new_full_path_no_meta_no_stamp.txt";
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

sub pf_new_full_path_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, some metadata, no timestamp
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pf_new_full_path_meta_no_stamp.txt";
  my $additional_avu1 = $irods->make_avu($RT_TICKET, '1234567890');
  my $additional_avu2 = $irods->make_avu($ANALYSIS_UUID,
                                         'abcdefg-01234567890-wxyz');

  is($publisher->publish_file($local_path_a, $remote_path,
                              [$additional_avu1, $additional_avu2]),
     $remote_path,
     'publish_file, full path, additional metadata, default timestamp');

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);

 is($obj->get_avu($DCTERMS_CREATOR)->{value}, 'http://www.sanger.ac.uk',
     'New object creator URI') or diag explain $obj->metadata;

  ok(URI->new($obj->get_avu($DCTERMS_PUBLISHER)->{value}), 'Publisher URI') or
    diag explain $obj->metadata;

  is($obj->get_avu($FILE_MD5)->{value}, 'a9fdbcfbce13a3d8dee559f58122a31c',
     'New object MD5') or diag explain $obj->metadata;

  is($obj->get_avu($RT_TICKET)->{value}, $additional_avu1->{value},
     'New additional AVU 1') or diag explain $obj->metadata;

  is($obj->get_avu($ANALYSIS_UUID)->{value}, $additional_avu2->{value},
     'New additional AVU 2') or diag explain $obj->metadata;
}

sub pf_new_full_path_no_meta_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with new full path, no metadata, no timestamp
  my $timestamp = DateTime->now;
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pf_new_full_path_no_meta_stamp.txt";

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

sub pf_exist_full_path_no_meta_no_stamp_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, no metadata, no timestamp,
  # matching MD5
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pf_exist_full_path_no_meta_no_stamp_match.txt";
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

sub pf_exist_full_path_meta_no_stamp_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, some metadata, no timestamp,
  # matching MD5
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path = "$coll_path/pf_exist_full_path_meta_no_stamp_match.txt";
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

sub pf_exist_full_path_no_meta_no_stamp_no_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, no metadata, no timestamp,
  # non-matching MD5
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path =
    "$irods_tmp_coll/pf_exist_full_path_no_meta_no_stamp_no_match";
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

sub pf_exist_full_path_meta_no_stamp_no_match {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_file with existing full path, some metadata, no timestamp,
  # non-matching MD5
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path_a = "$data_path/publish_file/a.txt";
  my $remote_path =
    "$irods_tmp_coll/pf_exist_full_path_meta_no_stamp_no_match.txt";
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

sub pd_new_full_path_no_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_directory with new full path, no metadata, no timestamp
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path = "$data_path/publish_directory";

  my $remote_path = "$coll_path/pd_new_full_path_no_meta_no_stamp";
  my $sub_coll = "$remote_path/publish_directory";
  is($publisher->publish_directory($local_path, $remote_path),
     $sub_coll,
     'publish_directory, full path, no additional metadata, default timestamp');

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $sub_coll);
  like($coll->get_avu($DCTERMS_CREATED)->{value}, qr{^$timestamp_regex$},
       'New object default creation timestamp') or diag explain $coll->metadata;

  is($coll->get_avu($DCTERMS_CREATOR)->{value}, 'http://www.sanger.ac.uk',
     'New object creator URI') or diag explain $coll->metadata;

  ok(URI->new($coll->get_avu($DCTERMS_PUBLISHER)->{value}), 'Publisher URI') or
    diag explain $coll->metadata;
}

sub pd_new_full_path_meta_no_stamp {
  my ($irods, $data_path, $coll_path) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $irods);

  # publish_directory with new full path, no metadata, no timestamp
  my $timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}';
  my $local_path = "$data_path/publish_directory";
  my $additional_avu1 = $irods->make_avu($RT_TICKET, '1234567890');
  my $additional_avu2 = $irods->make_avu($ANALYSIS_UUID,
                                         'abcdefg-01234567890-wxyz');

  my $remote_path = "$coll_path/pd_new_full_path_meta_no_stamp";
  my $sub_coll = "$remote_path/publish_directory";
  is($publisher->publish_directory($local_path, $remote_path,
                                   [$additional_avu1, $additional_avu2]),
     $sub_coll,
     'publish_directory, full path, no additional metadata, default timestamp');

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $sub_coll);
  like($coll->get_avu($DCTERMS_CREATED)->{value}, qr{^$timestamp_regex$},
       'New object default creation timestamp') or diag explain $coll->metadata;

  like($coll->get_avu($DCTERMS_CREATED)->{value}, qr{^$timestamp_regex$},
       'New object default creation timestamp') or diag explain $coll->metadata;

  is($coll->get_avu($DCTERMS_CREATOR)->{value}, 'http://www.sanger.ac.uk',
     'New object creator URI') or diag explain $coll->metadata;

  ok(URI->new($coll->get_avu($DCTERMS_PUBLISHER)->{value}), 'Publisher URI') or
    diag explain $coll->metadata;

  is($coll->get_avu($RT_TICKET)->{value}, $additional_avu1->{value},
     'New additional AVU 1') or diag explain $coll->metadata;

  is($coll->get_avu($ANALYSIS_UUID)->{value}, $additional_avu2->{value},
     'New additional AVU 2') or diag explain $coll->metadata;
}

1;
