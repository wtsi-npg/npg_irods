package WTSI::NPG::HTS::ONT::GridIONRunPublisherTest;

use strict;
use warnings;

use Archive::Tar;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[abs2rel catfile rel2abs];
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl;
use List::AllUtils qw[uniq];
use Sys::Hostname;
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

sub publish_files_copy : Test(83) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  _do_publish_files($irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_move : Test(83) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  _do_publish_files($irods_tmp_coll, 'move',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_f5_uncompress : Test(83) {
  my $expected_num_files = {fast5 => 7,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 6, 6, 6, 4], # total 40
                            fastq => [2]};

  my $f5_uncompress = 1;

 SKIP: {
    # h5repack fails with a 'file not found error', however, a
    # subsequent test for the file's presence using Perl's '-e' shows
    # it was there.
    skip 'h5repack not required', 83, if not $ENV{TEST_WITH_H5REPACK};

    _do_publish_files($irods_tmp_coll, 'copy',
                      $expected_num_files, $expected_num_items,
                      $f5_uncompress);
  }
}

sub publish_files_tar_bytes : Test(71) {
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
      $f5_uncompress, $arch_capacity, $arch_bytes) = @_;

  # File::Copy::copy uses open -> syswrite -> close, so we will get a
  # CLOSE event
  if (not ($data_mode eq 'copy' or $data_mode eq 'move')) {
    fail "Invalid data mode '$data_mode'";
  }

  $arch_capacity ||= 6;
  $arch_bytes    ||= 10_000_000;

  my $expt_name  = '2';
  my $expt_dir   = "$data_path/$expt_name";
  my $device_id  = "GA10000";
  my $device_dir = "$expt_dir/$device_id";

  # my $tmp_dir = File::Temp->newdir(DIR => '/tmp', CLEANUP => 0)->dirname;
  my $tmp_dir        = File::Temp->newdir->dirname;
  my $output_dir     = "$tmp_dir/output";
  my $basecalled_dir = "$tmp_dir/basecalled";
  make_path($output_dir);
  make_path($basecalled_dir);

  my @f5_tmp_dirs = ("$basecalled_dir/reads/0", "$basecalled_dir/reads/1");
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
       output_dir      => $output_dir,
       session_timeout => 30,
       source_dir      => $basecalled_dir,
       f5_uncompress   => $f5_uncompress);

    my ($num_files, $num_processed, $num_errors) = $pub->publish_files;

    exit $num_errors;
  }

  sleep 5;

  my @data_files;
  foreach my $dir ("$device_dir/reads/0",
                   "$device_dir/reads/1",
                   $device_dir) {
    opendir my $dh, $dir or die "Failed to opendir '$dir': $!";
    my @files = sort map { catfile($dir, $_) }
      grep { m{[.](cfg|fast5|fastq|txt)$}msx } readdir $dh;
    closedir $dh;

    push @data_files, @files;
  }

  # Simulate writing new fast5, fastq, cfg and txt files
  foreach my $file (@data_files) {
    my $tmp_file = rel2abs(abs2rel($file, $device_dir), $basecalled_dir);

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
    my $expected_manifest = catfile($output_dir, $manifest_file);

    # Check manifests exist
    ok(-e $expected_manifest, "Manifest file '$expected_manifest' exists");

    # Check they contain the correct number of entries
    my $num_tar_files = $expected_num_files->{$format};
    my %manifest = _read_manifest($expected_manifest);
    cmp_ok(scalar uniq(values %manifest), '==', $num_tar_files,
           "Manifest lists $num_tar_files tar files") or
             diag explain \%manifest;

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
      my $path     = $obj->str;
      my $filename = fileparse($path);
      my $file     = $irods->get_object($path, catfile($tmp_dir, $filename));

      # Check the MD5 metadata
      my @avu = $obj->find_in_metadata($FILE_MD5);
      cmp_ok(scalar @avu, '==', 1,
             "Single MD5 attribute present on '$path'") or diag explain \@avu;

      my $md5_meta = $avu[0]->{value};
      my $md5      = $irods->md5sum($file);
      is($md5_meta, $md5, "MD5 metadata and file checksum concur for '$path'")
        or diag explain [$md5_meta, $md5];

      # Check that the tar file contains the right number of items
      my $arch = Archive::Tar->new;
      $arch->read($file);
      my @items = $arch->list_files;

      my $expected_num_items = $expected_num_items->{$format}[$i];
      cmp_ok(scalar @items, '==', $expected_num_items,
             "Expected expected number of items present in '$path'");

      # Check that the manifest describe the tar file contents
      my $manifest_fail = 0;
      foreach my $item (@items) {
        my $tar = $manifest{$item};
        is($path, $tar, "Manifest describes tar file for '$item'") or
          $manifest_fail++;
      }

      if ($manifest_fail) {
        diag explain \%manifest;
      }

      my $expected_meta =
        [{attribute => 'device_id',       value => 'GA10000'},
         {attribute => 'experiment_name', value => '2'},
         {attribute => 'type',            value => 'tar'}];

      my @filtered_meta = grep { $_->{attribute} !~ m{(md5|dcterms)}msx }
        @{$obj->metadata};

      is_deeply(\@filtered_meta, $expected_meta,
                "Expected metadata present on '$path'")
        or diag explain \@filtered_meta;

      $i++;
    }
  }
}

sub _read_manifest {
  my ($path) = @_;

  my %manifest;
  open my $fh, '<', $path or die "Failed to open manifest '$path': $ERRNO";
  while (my $line = <$fh>) {
    chomp $line;

    my ($tar_path, $item_path) = split /\t/msx, $line;
    $manifest{$item_path} = $tar_path;
  }
  close $fh or die "Failed to close '$path': $ERRNO";

  return %manifest;
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
