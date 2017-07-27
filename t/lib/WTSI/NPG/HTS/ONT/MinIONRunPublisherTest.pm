package WTSI::NPG::HTS::ONT::MinIONRunPublisherTest;

use strict;
use warnings;

use Archive::Tar;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[catfile];
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
  # $irods->remove_collection($irods_tmp_coll);
}


sub publish_files : Test(40) {
  my $session_name  = 'test';
  my $f5_uncompress = 0;

  _do_publish_files($session_name, $irods_tmp_coll, $f5_uncompress);
}

sub publish_files_f5_uncompress : Test(40) {
  my $session_name  = 'test_uncompress';
  my $f5_uncompress = 1;

 TODO: {
    # h5repack fails with a 'file not found error', however, a
    # subsequent test for the file's presence usoing Perl's '-e' shows
    # it was there.
    local $TODO = 'h5repack sometimes fails to open the input';

    _do_publish_files($session_name, $irods_tmp_coll, $f5_uncompress);
  }
}

sub _do_publish_files {
  my ($session_name, $dest_coll, $f5_uncompress) = @_;

  my $staging_dir    = File::Temp->newdir->dirname;
  my $runfolder_path = "$staging_dir/$run_name";
  my $f5_pass_dir    = "$runfolder_path/fast5/pass/0";
  my $fq_pass_dir    = "$runfolder_path/fastq/pass";
  make_path($f5_pass_dir);
  make_path($fq_pass_dir);

  my $arch_capacity = 6;

  my $pid = fork();
  die "Failed to fork a test process" unless defined $pid;

  if ($pid == 0) {
    my $pub = WTSI::NPG::HTS::ONT::MinIONRunPublisher->new
      (arch_capacity   => $arch_capacity,
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
  foreach my $file (@{$fast5_files}) {
    copy($file, $f5_pass_dir) or die "Failed to copy $file: $ERRNO";
  }
  foreach my $file (@{$fastq_files}) {
    copy($file, $fq_pass_dir) or die "Failed to copy $file: $ERRNO";
  }

  waitpid($pid, 0);
  cmp_ok($CHILD_ERROR, '==', 0, 'Publisher exited cleanly');

  # Check the manifests
  my %expected_num_files = (fast5 => 4,
                            fastq => 1);
  my %expected_num_items = (fast5 => [6, 6, 6, 2],
                            fastq => [4]);

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

    my $n = $expected_num_files{$format};
    cmp_ok(scalar uniq(values %manifest), '==', $n,
           "Manifest lists $n tar files") or diag explain \%manifest;

    # Count the tar files created in iRODS
    my $irods = WTSI::NPG::iRODS->new;

    my $tar_coll = "$dest_coll";
    my ($observed_paths) = $irods->list_collection($tar_coll);
    my @observed_paths = grep { m{$format[.]\d+[.]tar$}msx }
      @{$observed_paths};

    cmp_ok(scalar @observed_paths, '==', $n, "Published $n $format tar files")
      or diag explain \@observed_paths;

    # Fetch the MD5 metadata
    my @observed_md5_metadata;
    foreach my $path (@observed_paths) {
      my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $path);
      my @avu = $obj->find_in_metadata($FILE_MD5);
      cmp_ok(scalar @avu, '==', 1, 'Single md5 attribute present');
      push @observed_md5_metadata, $avu[0]->{value};
    }

    # Fetch the tar file data objects from iRODS and calculate their MD5s
    my @observed_md5_checksums;
    my @observed_file_counts;
    foreach my $tar (@observed_paths) {
      my $filename = fileparse($tar);
      my $file = $irods->get_object($tar, catfile($staging_dir, $filename));

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
        is($tar, $manifest_tar,
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

    is_deeply(\@observed_file_counts, $expected_num_items{$format} ,
              "$format tar file contains expected number of items") or
                diag explain \@observed_file_counts;
  }
}

1;
