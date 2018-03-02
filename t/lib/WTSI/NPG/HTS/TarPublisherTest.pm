package WTSI::NPG::HTS::TarPublisherTest;

use strict;
use warnings;

use Archive::Tar;
use Digest::MD5;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::TarPublisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/tar_publisher';

my $irods_tmp_coll;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("TarPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub publish_file : Test(21) {
  my $tmp_dir       = File::Temp->newdir->dirname;
  my $manifest_path = "$tmp_dir/manifest.txt";
  my $tar_path      = "$irods_tmp_coll/test";
  my $tar_cwd       = $data_path;

  my $publisher = WTSI::NPG::HTS::TarPublisher->new
    (manifest_path => $manifest_path,
     remove_files  => 1,
     tar_bytes     => 1_000_000,
     tar_capacity  => 2,
     tar_cwd       => $tmp_dir,
     tar_path      => $tar_path);

  make_path("$tmp_dir/a");
  copy("$data_path/aaa.txt", "$tmp_dir/a/aaa.txt");
  my $md5 = '5c9597f3c8245907ea71a89d9d39d08e';

  ok(!$publisher->tar_in_progress, "Initially no tar in progress");

  is($publisher->publish_file("$tmp_dir/a/aaa.txt"),
     "$tar_path.0.tar", "Published to correct tar file");

  ok($publisher->tar_in_progress, "Tar in progress");

  ok($publisher->file_published("$tmp_dir/a/aaa.txt"),
     "File recorded as published");

  ok(!$publisher->file_updated("$tmp_dir/a/aaa.txt"),
     "File not recorded as updated");

  cmp_ok($publisher->tar_count, '==', 0, "Correct initial tar count");
  cmp_ok($publisher->tar_stream->file_count, '==', 1, "Correct file count");
  cmp_ok($publisher->tar_stream->byte_count, '==', 4, "Correct byte count");

  $publisher->close_stream;

  ok(!$publisher->tar_in_progress, "Finally no tar in progress");

  cmp_ok($publisher->tar_count, '==', 1, "Correct final tar count");

  isnt(-e "$tmp_dir/a/aaa.txt", 'Tar has removed input file');

  my $manifest = $publisher->tar_manifest;
  ok($manifest->file_exists, "Manifest file was written");
  ok($manifest->contains_item("a/aaa.txt"),
     "Manifest contains correct item");
  cmp_ok(scalar $manifest->item_paths, '==', 1,
         "Manifest contains expected number of items");

  my $manifest_item = $manifest->get_item("a/aaa.txt");
  is($manifest_item->item_path, "a/aaa.txt",
     "Manifest item has expected item path") or
       diag explain $manifest_item->item_path;
  is($manifest_item->tar_path, "$tar_path.0.tar",
     "Manifest item has expected tar path") or
       diag explain $manifest_item->tar_path;
  is($manifest_item->checksum, $md5,
     "Manifest item has expected checksum") or
       diag explain $manifest_item->checksum;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $tar_file = $irods->get_object("$tar_path.0.tar", "$tmp_dir/test.0.tar");
  my $arch = Archive::Tar->new;
  $arch->read($tar_file);

  my @tar_items = $arch->get_files;
  cmp_ok(scalar @tar_items, '==', 1,
         "Tar file contains expected number of items");

  my $item = $tar_items[0];
  is($item->name, "a/aaa.txt", "Tar item is correct");
  is($item->get_content, "aaa\n", "Tar item has the correct content") or
    diag explain $item->get_content;
  is(Digest::MD5->new->add($item->get_content)->hexdigest, $md5,
     "Tar item has correct checksum") or
       diag explain $item->get_content;
}

sub update_file_same_tar : Test(6) {
  my $tmp_dir       = File::Temp->newdir->dirname;
  my $manifest_path = "$tmp_dir/manifest.txt";
  my $tar_path      = "$irods_tmp_coll/test";
  my $tar_cwd       = $data_path;

  my $publisher = WTSI::NPG::HTS::TarPublisher->new
    (manifest_path => $manifest_path,
     remove_files  => 1,
     tar_bytes     => 1_000_000,
     tar_capacity  => 2,
     tar_cwd       => $tmp_dir,
     tar_path      => $tar_path);

  make_path("$tmp_dir/a");

  copy("$data_path/aaa.txt", "$tmp_dir/a/aaa.txt");
  is($publisher->publish_file("$tmp_dir/a/aaa.txt"),
     "$tar_path.0.tar", "Published to correct tar file 1");
  ok(!$publisher->file_updated("$tmp_dir/a/aaa.txt"),
     "File not recorded as updated");

  copy("$data_path/bbb.txt", "$tmp_dir/a/aaa.txt");
  is($publisher->publish_file("$tmp_dir/a/aaa.txt"),
     "$tar_path.0.tar", "Published to correct tar file 2");
  ok($publisher->file_updated("$tmp_dir/a/aaa.txt"),
     "File is recorded as updated");

  $publisher->close_stream;
  isnt(-e "$tmp_dir/a/aaa.txt", 'Tar has removed input file');
  cmp_ok($publisher->tar_count, '==', 1, "Correct final tar count");
}

sub update_file_diff_tar : Test(8) {
  my $tmp_dir       = File::Temp->newdir->dirname;
  my $manifest_path = "$tmp_dir/manifest.txt";
  my $tar_path      = "$irods_tmp_coll/test";
  my $tar_cwd       = $data_path;

  my $publisher = WTSI::NPG::HTS::TarPublisher->new
    (manifest_path => $manifest_path,
     remove_files  => 1,
     tar_bytes     => 1_000_000,
     tar_capacity  => 2,
     tar_cwd       => $tmp_dir,
     tar_path      => $tar_path);

  make_path("$tmp_dir/a");
  copy("$data_path/aaa.txt", "$tmp_dir/a/aaa.txt");

  is($publisher->publish_file("$tmp_dir/a/aaa.txt"),
     "$tar_path.0.tar", "Published to correct tar file 1");
  ok(!$publisher->file_updated("$tmp_dir/a/aaa.txt"),
     "File not recorded as updated");

  $publisher->close_stream;
  isnt(-e "$tmp_dir/a/aaa.txt", 'Tar has removed input file');
  cmp_ok($publisher->tar_count, '==', 1, "Correct final tar count");

  copy("$data_path/bbb.txt", "$tmp_dir/a/aaa.txt");
  is($publisher->publish_file("$tmp_dir/a/aaa.txt"),
     "$tar_path.1.tar", "Published to correct tar file 2");
  ok($publisher->file_updated("$tmp_dir/a/aaa.txt"),
     "File is recorded as updated");

  $publisher->close_stream;
  isnt(-e "$tmp_dir/a/aaa.txt", 'Tar has removed input file');
  cmp_ok($publisher->tar_count, '==', 2, "Correct final tar count");
}

1;
