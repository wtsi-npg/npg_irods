package WTSI::NPG::HTS::ONT::MinIONRunPublisherTest;

use strict;
use warnings;

use Archive::Tar;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[catdir catfile];
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl;
use List::AllUtils qw[uniq];
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::ONT::MinIONRunPublisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/ont/minion/1.7.3/data';
my $run_name     = '20170629_1348_pc3_linuxtext';
my $run_id       = '9c461c741cb14362e613136e235aa38b67ef2f6d';

my $irods_tmp_coll;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("MinIONRunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub publish_files_copy : Test(55) {
  my $session_name  = 'test';
  my $expected_num_files = {fast5 => 4,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 2],
                            fastq => [4]};

  _do_publish_files($session_name, $irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_move : Test(55) {
  my $session_name  = 'test';
  my $expected_num_files = {fast5 => 4,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 2],
                            fastq => [4]};

  _do_publish_files($session_name, $irods_tmp_coll, 'move',
                    $expected_num_files, $expected_num_items);
}

sub publish_files_f5_uncompress : Test(55) {
  my $session_name  = 'test_uncompress';
  my $expected_num_files = {fast5 => 4,
                            fastq => 1};
  my $expected_num_items = {fast5 => [6, 6, 6, 2],
                            fastq => [4]};

  my $f5_uncompress = 1;

 SKIP: {
    # h5repack fails with a 'file not found error', however, a
    # subsequent test for the file's presence using Perl's '-e' shows
    # it was there.
    skip 'h5repack not required', 55, if not $ENV{TEST_WITH_H5REPACK};

    _do_publish_files($session_name, $irods_tmp_coll, 'copy',
                      $expected_num_files, $expected_num_items,
                      $f5_uncompress);
  }
}

sub publish_files_tar_bytes : Test(52) {
  my $session_name  = 'test_tar_bytes';
  my $expected_num_files = {fast5 => 4,
                            fastq => 1};
  my $expected_num_items = {fast5 => [5, 4, 5, 3],
                            fastq => [4]};

  my $f5_uncompress = 0;
  my $arch_capacity = 6;
  my $arch_bytes    = 2_500_000;

  _do_publish_files($session_name, $irods_tmp_coll, 'copy',
                    $expected_num_files, $expected_num_items,
                    $f5_uncompress, $arch_capacity, $arch_bytes);
}

sub _do_publish_files {
  my ($session_name, $dest_coll, $data_mode,
      $expected_num_files, $expected_num_items,
      $f5_uncompress, $arch_capacity, $arch_bytes) = @_;

  # File::Copy::copy uses open -> syswrite -> close, so we will get a
  # CLOSE event
  if (not ($data_mode eq 'copy' or $data_mode eq 'move')) {
    fail "Invalid data mode '$data_mode'";
  }

  $arch_capacity ||= 6;
  $arch_bytes    ||= 10_000_000;

  my $staging_dir    = File::Temp->newdir->dirname;
  my $runfolder_path = "$staging_dir/$run_name";
  my $f5_pass_dir    = "$runfolder_path/fast5/pass/0";
  my $fq_pass_dir    = "$runfolder_path/fastq/pass";
  make_path($f5_pass_dir);
  make_path($fq_pass_dir);

  my $pid = fork();
  die "Failed to fork a test process" unless defined $pid;

  if ($pid == 0) {
    my $pub = WTSI::NPG::HTS::ONT::MinIONRunPublisher->new
      (arch_bytes      => $arch_bytes,
       arch_capacity   => $arch_capacity,
       arch_timeout    => 10,
       dest_collection => $dest_coll,
       runfolder_path  => $runfolder_path,
       session_name    => $session_name,
       session_timeout => 30,
       f5_uncompress   => $f5_uncompress);

    my ($tar_count, $num_errors) = $pub->publish_files;

    exit $num_errors;
  }

  sleep 5;

  my @fastx_files;
  foreach my $path ("$data_path/reads/$run_name/fast5/pass/0",
                    "$data_path/reads/$run_name/fastq/pass") {
    opendir my $dh, $path or die "Failed to opendir '$path': $!";
    my @files = map { catfile($path, $_) } grep { m{[.]fast}msx } readdir $dh;
    closedir $dh;

    push @fastx_files, \@files;
  }

  my ($fast5_files, $fastq_files) = @fastx_files;

  my $fast5_count = scalar @$fast5_files;
  my $fastq_count = scalar @$fastq_files;

  # Simulate writing new fast5 and fastq files
  foreach my $file (sort @{$fast5_files}) {
    if ($data_mode eq 'copy') {
      sleep 1;
      copy($file, $f5_pass_dir) or die "Failed to copy $file: $ERRNO";
    }
    elsif ($data_mode eq 'move') {
      sleep 1;
      _move($file, $f5_pass_dir);
    }
  }
  foreach my $file (sort @{$fastq_files}) {
    if ($data_mode eq 'copy') {
      sleep 1;
      copy($file, $fq_pass_dir) or die "Failed to copy $file: $ERRNO";
    }
    elsif ($data_mode eq 'move') {
      sleep 1;
      _move($file, $fq_pass_dir);
    }
  }

  waitpid($pid, 0);
  cmp_ok($CHILD_ERROR, '==', 0, 'Publisher exited cleanly');

  # Check the manifests
  my $irods = WTSI::NPG::iRODS->new;
  my $run_coll = catdir($dest_coll, $run_id);
  my $tar_coll = WTSI::NPG::iRODS::Collection->new($irods, $run_coll);

  ok($tar_coll->get_avu($ID_RUN, $run_id),
     "Collection '$run_coll' has expected $ID_RUN");
  ok($tar_coll->get_avu($SAMPLE_NAME, 'pc3_linuxtext'),
     "Collection '$run_coll' has expected $SAMPLE_NAME");
  foreach my $attr ($DCTERMS_CREATED, $DCTERMS_CREATOR, $DCTERMS_PUBLISHER) {
    my @avu = $tar_coll->find_in_metadata($attr);
    cmp_ok(scalar @avu, '==', 1,
           "Collection $attr metadata present on collection '$run_coll'");
  }

  my ($objs, $colls) = $tar_coll->get_contents;

  foreach my $format (qw[fast5 fastq]) {
    my $manifest_file = sprintf '%s_%s_manifest.txt', $run_id, $format;
    my $expected_manifest = "$runfolder_path/$manifest_file";

    # Check manifests exist
    ok(-e $expected_manifest, "Manifest file '$expected_manifest' exists");

    # Check they contain the correct number of entries
    my %manifest;
    open my $fh, '<', $expected_manifest or
      die "Failed to open manifest '$expected_manifest': $ERRNO";
    while (my $line = <$fh>) {
      chomp $line;

      my ($tar_path, $item_path) = split /\t/msx, $line;
      $manifest{$item_path} = $tar_path;
    }
    close $fh or die "Failed to close '$expected_manifest': $ERRNO";

    my $n = $expected_num_files->{$format};
    cmp_ok(scalar uniq(values %manifest), '==', $n,
           "Manifest lists $n tar files") or diag explain \%manifest;

    my @observed_objs = grep { $_->str =~ m{$format[.]\d+[.]tar$}msx } @{$objs};

    # Count the tar files created in iRODS
    cmp_ok(scalar @observed_objs, '==', $n, "Published $n $format tar files")
      or diag explain \@observed_objs;

    # Fetch the MD5 metadata
    my @observed_md5_metadata;
    foreach my $obj (@observed_objs) {
      my @avu = $obj->find_in_metadata($FILE_MD5);
      cmp_ok(scalar @avu, '==', 1, 'Single md5 attribute present');
      push @observed_md5_metadata, $avu[0]->{value};
    }

    # Fetch the tar file data objects from iRODS and calculate their MD5s
    my @observed_md5_checksums;
    my @observed_file_counts;
    foreach my $tar (@observed_objs) {
      my $filename = fileparse($tar->str);
      my $file =
        $irods->get_object($tar->str, catfile($staging_dir, $filename));

      my $md5 = $irods->md5sum($file);
      push @observed_md5_checksums, $md5;

      my $arch = Archive::Tar->new;
      $arch->read($file);
      my @entries = $arch->list_files;
      push @observed_file_counts, scalar @entries;

      my $manifest_fail = 0;
      foreach my $entry (@entries) {
        # Look up the tar file iRODS data object in the manifest
        my $manifest_tar = $manifest{$entry};
        is($tar->str, $manifest_tar,
           "Manifest describes tar file for '$entry'") or $manifest_fail++;
      }

      if ($manifest_fail) {
        diag explain \%manifest;
      }
    }

    is_deeply(\@observed_md5_checksums, \@observed_md5_metadata,
              "$format tar file MD5 checksums and metadata concur") or
                diag explain [\@observed_md5_checksums,
                              \@observed_md5_metadata];

    is_deeply(\@observed_file_counts, $expected_num_items->{$format} ,
              "$format tar file contains expected number of items") or
                diag explain [\@observed_file_counts,
                              \%manifest];

    # Check the other metadata
    check_primary_metadata($irods, @observed_objs);
  }
}

sub check_primary_metadata {
  my ($irods, @objs) = @_;

  foreach my $obj (@objs) {
    my $file_name = fileparse($obj->str);

    foreach my $attr ($DCTERMS_CREATED, $FILE_TYPE) {
      my @avu = $obj->find_in_metadata($attr);
      cmp_ok(scalar @avu, '==', 1, "$file_name $attr metadata present");
    }
  }
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
