package WTSI::NPG::HTS::ONT::GridIONRunPublisherTest;

use strict;
use warnings;

use Archive::Tar;
use Digest::MD5;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[abs2rel catfile rel2abs];
use File::Path qw[make_path];
use File::Temp;
use IO::Uncompress::Bunzip2 qw[bunzip2 $Bunzip2Error];
use Log::Log4perl;
use List::AllUtils qw[uniq];
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::ONT::GridIONRunPublisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/ont/gridion';
my $fixture_path = "t/fixtures";
my $db_dir       = File::Temp->newdir;

my $wh_schema;

my $irods_tmp_coll;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("GridIONRunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub publish_files_copy : Test(125) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  _do_publish_files($irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_move : Test(125) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  _do_publish_files($irods_tmp_coll, 'move',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_single_server : Test(125) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};
  my $f5_uncompress = 0;
  my $arch_capacity ||= 6;
  my $arch_bytes    ||= 10_000_000;
  my $single_server = 1;

  _do_publish_files($irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items,
                    $f5_uncompress, $arch_capacity, $arch_bytes,
                    $single_server);
}

sub publish_files_f5_uncompress : Test(125) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  my $f5_uncompress = 1;

 SKIP: {
    # h5repack fails with a 'file not found error', however, a
    # subsequent test for the file's presence using Perl's '-e' shows
    # it was there.
    skip 'h5repack not required', 125, if not $ENV{TEST_WITH_H5REPACK};

    _do_publish_files($irods_tmp_coll, 'copy',
                      $expected_num_files, $expected_num_items,
                      $f5_uncompress);
  }
}

sub publish_files_tar_bytes : Test(113) {
  my $expected_num_files = {fast5 => 4,
                            fastq => 1};
  my $expected_num_items = {fast5 => [23, 8, 7, 2], # total 40
                            fastq => [2]};

  my $f5_uncompress = 0;
  my $arch_capacity = 100;
  my $arch_bytes    = 500_000;

  _do_publish_files($irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items,
                    $f5_uncompress, $arch_capacity, $arch_bytes);
}

sub _do_publish_files {
  my ($dest_coll, $data_mode,
      $expected_num_files, $expected_num_items,
      $f5_uncompress, $arch_capacity, $arch_bytes,
      $single_server) = @_;

  # File::Copy::copy uses open -> syswrite -> close, so we will get a
  # CLOSE event
  if (not ($data_mode eq 'copy' or $data_mode eq 'move')) {
    fail "Invalid data mode '$data_mode'";
  }

  $arch_capacity ||= 6;
  $arch_bytes    ||= 10_000_000;

  my $expt_name    = '2';
  my $device_id    = "GA10000";
  my $data_run_dir = "$data_path/$expt_name/$device_id";

  # my $tmp_dir = File::Temp->newdir(DIR => '/tmp', CLEANUP => 0)->dirname;
  my $tmp_dir            = File::Temp->newdir->dirname;
  my $tmp_output_dir     = "$tmp_dir/output";
  my $tmp_basecalled_dir = "$tmp_dir/basecalled";
  my $tmp_run_dir        = "$tmp_basecalled_dir/$expt_name/$device_id";
  make_path($tmp_output_dir);
  make_path($tmp_run_dir);

  my @f5_tmp_dirs = ("$tmp_run_dir/reads/0", "$tmp_run_dir/reads/1");
  foreach my $f5_tmp_dir (@f5_tmp_dirs) {
    make_path($f5_tmp_dir);
  }

  my $pid = fork();
  die "Failed to fork a test process" unless defined $pid;

  if ($pid == 0) {
    my $pub = WTSI::NPG::HTS::ONT::GridIONRunPublisher->new
      (arch_bytes      => $arch_bytes,
       arch_capacity   => $arch_capacity,
       arch_timeout    => 10,
       dest_collection => $dest_coll,
       device_id       => $device_id,
       experiment_name => $expt_name,
       f5_uncompress   => $f5_uncompress,
       output_dir      => $tmp_output_dir,
       session_timeout => 30,
       single_server   => $single_server,
       source_dir      => $tmp_run_dir);

    my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

    exit $num_errors;
  }

  sleep 5;

  my @data_files;
  foreach my $dir ("$data_run_dir/reads/0",
                   "$data_run_dir/reads/1",
                   $data_run_dir) {
    opendir my $dh, $dir or die "Failed to opendir '$dir': $!";
    my @files = sort map { catfile($dir, $_) }
      grep { m{[.](cfg|fast5|fastq|txt)$}msx } readdir $dh;
    closedir $dh;

    push @data_files, @files;
  }

  # Simulate writing new fast5, fastq, cfg and txt files
  foreach my $file (@data_files) {
    my $tmp_file = rel2abs(abs2rel($file, $data_run_dir), $tmp_run_dir);

    if ($data_mode eq 'copy') {
      copy($file, $tmp_file) or die "Failed to copy $file: $ERRNO";
    }
    elsif ($data_mode eq 'move') {
      _move($file, $tmp_file);
    }
  }

  waitpid($pid, 0);
  cmp_ok($CHILD_ERROR, '==', 0, 'Publisher exited cleanly');

  # Check the manifests
  my $irods = WTSI::NPG::iRODS->new;
  my ($dest_objs, $dest_colls) =
    WTSI::NPG::iRODS::Collection->new($irods, $dest_coll)->get_contents(1);

  my ($tar_coll) = grep { $_->str =~ m{$device_id$}msx } @$dest_colls;

  my ($objs, $colls) = $tar_coll->get_contents;

  foreach my $format (qw[fast5 fastq]) {
    # For this data format
    my $manifest_file = sprintf '%s_%s_%s_manifest.txt',
      $expt_name, $device_id, $format;
    my $expected_manifest = catfile($tmp_output_dir, $manifest_file);

    # Check manifests exist
    ok(-e $expected_manifest, "Manifest file '$expected_manifest' exists");

    # Check they contain the correct number of entries
    my $num_tar_files = $expected_num_files->{$format};
    my $manifest = _read_manifest($expected_manifest);
    cmp_ok(scalar $manifest->tar_paths, '==', $num_tar_files,
           "Manifest lists $num_tar_files tar files") or
             diag explain [$manifest->tar_paths];

    # Check the ancillary files in iRODS
    my $fn = sub {
      my $obj = shift;
      my ($f) = fileparse($obj->str);
      return $f;
    };

    my @observed_ancillary = sort { $a cmp $b }
      grep { m{[.](cfg|txt)$}msx }
      map { $fn->($_) } @$objs;
    is_deeply(\@observed_ancillary, ['2_GA10000_fast5_manifest.txt',
                                     '2_GA10000_fastq_manifest.txt',
                                     'configuration.cfg',
                                     'sequencing_summary_0.txt',
                                     'sequencing_summary_1.txt'])
      or diag explain \@observed_ancillary;

    # Count the tar files created in iRODS
    my @observed_tar = sort { $a->str cmp $b->str }
      grep { $_->str =~ m{$format\S+[.]\d+[.]tar$}msx } @$objs;
    cmp_ok(scalar @observed_tar, '==', $num_tar_files,
           "Published $num_tar_files $format tar files")
      or diag explain [ map { $_->str } @observed_tar];

    my $i = 0;
    foreach my $obj (@observed_tar) {
      my $tar_path = $obj->str;
      my $filename = fileparse($tar_path);

      # Get the tar file from iRODS
      my $tar_file = $irods->get_object($tar_path,
                                        catfile($tmp_dir, $filename));
      # Check the MD5 metadata
      my @avu = $obj->find_in_metadata($FILE_MD5);
      cmp_ok(scalar @avu, '==', 1,
             "Single MD5 attribute present on '$tar_path'") or
               diag explain \@avu;
      my $md5      = $irods->md5sum($tar_file);
      my $md5_meta = $avu[0]->{value};

      is($md5_meta, $md5,
         "MD5 metadata and file checksum concur for '$tar_path'") or
           diag explain [$md5_meta, $md5];

      # Check that the tar file contains the right number of items
      my $arch = Archive::Tar->new;
      $arch->read($tar_file);
      my @tar_items = $arch->get_files;

      my $expected_num_items = $expected_num_items->{$format}[$i];
      cmp_ok(scalar @tar_items, '==', $expected_num_items,
             "Expected expected number of items present in '$tar_path'");

      # Check that the manifest describes each item of the tar file contents
      my $manifest_fail = 0;
      foreach my $tar_item (@tar_items) {
        my $item_name    = $tar_item->name;
        my $item_content = $tar_item->get_content;

        # Extract the item's record from the manifest
        my $man_item     = $manifest->get_item($item_name);

        is($man_item->tar_path, $tar_path,
           "Manifest describes tar file for '$item_name' as $tar_path") or
             $manifest_fail++;

        # Calculate the MD5 of each file contained in the tar file
        my $bunzipped_content;
        bunzip2 \$item_content => \$bunzipped_content or
          die "Failed to bunzip $item_name in $tar_file: $Bunzip2Error";

        my $tar_item_md5 =
          Digest::MD5->new->add($bunzipped_content)->hexdigest;
        is($man_item->checksum, $tar_item_md5,
           "Manifest describes checksum for '$item_name' as $tar_item_md5") or
             $manifest_fail++;
      }

      if ($manifest_fail) {
        diag explain $manifest;
      }

      my $expected_meta =
        [{attribute => 'device_id',       value => 'GA10000'},
         {attribute => 'experiment_name', value => '2'},
         {attribute => 'type',            value => 'tar'}];

      my @filtered_meta = grep { $_->{attribute} !~ m{(md5|dcterms)}msx }
        @{$obj->metadata};

      is_deeply(\@filtered_meta, $expected_meta,
                "Expected metadata present on '$tar_path'")
        or diag explain \@filtered_meta;

      $i++;
    }
  }
}

sub _read_manifest {
  my ($path) = @_;

  my $manifest = WTSI::NPG::HTS::TarManifest->new(manifest_path => $path);
  $manifest->read_file;

  return $manifest;
}

sub _move {
  my ($source, $dest) = @_;

  my ($file_name) = fileparse($source);

  my $tmp_dir  = File::Temp->newdir->dirname;
  make_path($tmp_dir);

  my $tmp_file = catfile($tmp_dir, $file_name);

  copy($source, $tmp_file) or
    die "Failed to copy '$source' to '$tmp_file': $ERRNO";

  my $dest_path;
  if (-d $dest) {
    $dest_path = catfile($dest, $file_name);
  }
  else {
    $dest_path = $dest;
  }

  move($tmp_file, $dest_path) or
    die "Failed to move '$tmp_file' to '$dest_path': $ERRNO";
}

1;
