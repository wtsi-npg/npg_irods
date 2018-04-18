package WTSI::NPG::HTS::ONT::GridIONTarAuditorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[abs2rel catfile rel2abs];
use File::Path qw[make_path];
use File::Temp;
use Sys::Hostname;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::ONT::GridIONRunPublisher;
use WTSI::NPG::HTS::ONT::GridIONTarAuditor;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS;

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
    $irods->add_collection("GridIONTarAuditorTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub check_all_files : Test(21) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $expt_name    = '2';
  my $device_id    = "GA10000";
  my $data_run_dir = "$data_path/$expt_name/$device_id";

  my $tmp_dir            = File::Temp->newdir->dirname;
  my $tmp_output_dir     = "$tmp_dir/output";
  my $tmp_basecalled_dir = "$tmp_dir/basecalled";
  my $tmp_run_dir        = "$tmp_basecalled_dir/$expt_name/$device_id";

  _do_publish_files($expt_name, $device_id, $data_run_dir,
                    $tmp_run_dir, $tmp_output_dir, $irods_tmp_coll);

  my $gridion_name  = hostname;
  my $dest_coll     = "$irods_tmp_coll/$gridion_name/$expt_name/$device_id";

  my $auditor = WTSI::NPG::HTS::ONT::GridIONTarAuditor->new
    (dest_collection => $dest_coll);

  my @f5_manifests = @{$auditor->f5_manifests};
  cmp_ok(scalar @f5_manifests, '==', 1, "Expected number of f5 manifests") or
    diag explain \@f5_manifests;

  my @fq_manifests = @{$auditor->fq_manifests};
  cmp_ok(scalar @fq_manifests, '==', 1, "Expected number of fq manifests") or
    diag explain \@fq_manifests;


  foreach my $f5_manifest (@f5_manifests) {
    my @observed_f5_tar = $f5_manifest->tar_paths;
    cmp_ok(scalar @observed_f5_tar, '==', 7,
           "Expected number of f5 tar files") or
             diag explain \@observed_f5_tar;

    foreach my $tar_path (@observed_f5_tar) {
      my $pattern = sprintf '^%s/%s_fast5_\d+-\d+-\d+T\d+\.\d+\.tar$',
        $dest_coll, $device_id;
      like($tar_path, qr{$pattern}, "f5 tar path matches $pattern");
    }
  }

  foreach my $fq_manifest (@fq_manifests) {
    my @observed_fq_tar = $fq_manifest->tar_paths;
    cmp_ok(scalar @observed_fq_tar, '==', 1,
           "Expected number of fq tar files") or
             diag explain \@observed_fq_tar;

    foreach my $tar_path (@observed_fq_tar) {
      my $pattern = sprintf '^%s/%s_fastq_\d+-\d+-\d+T\d+\.\d+\.tar$',
        $dest_coll, $device_id;
      like($tar_path, qr{$pattern}, "fq tar path matches $pattern");
    }
  }

  my ($nf5, $np5, $ne5) = $auditor->check_f5_tar_files;
  cmp_ok($nf5, '==', 40, "$nf5 f5 tarred f5 files");
  cmp_ok($np5, '==', 40, "$np5 f5 tarred f5 files processed");
  cmp_ok($ne5, '==',  0, "$ne5 errors in tarred f5 files");

  my ($nfq, $npq, $neq) = $auditor->check_fq_tar_files;
  cmp_ok($nfq, '==',  2, "$nfq fq tarred fq files");
  cmp_ok($npq, '==',  2, "$npq fq tarred fq files processed");
  cmp_ok($neq, '==',  0, "$ne5 errors in tarred fq files");

  my ($nf, $np, $ne) = $auditor->check_all_files;
  cmp_ok($nf, '==', 42, "$nf f5 tarred files");
  cmp_ok($np, '==', 42, "$np f5 tarred files processed");
  cmp_ok($ne, '==',  0, "$ne errors in tarred files");
}

sub _do_publish_files {
  my ($expt_name, $device_id, $data_run_dir, $tmp_run_dir, $tmp_output_dir,
      $dest_coll) = @_;

  my $arch_capacity = 6;
  my $arch_bytes    = 10_000_000;
  my $f5_uncompress = 0;

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
    copy($file, $tmp_file) or die "Failed to copy $file: $ERRNO";
  }

  waitpid($pid, 0);
  $CHILD_ERROR and die "GridIONRunPublisher child process failed: $ERRNO";
}

1;
