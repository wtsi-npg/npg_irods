package WTSI::NPG::HTS::ONT::GridIONTarAuditorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
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
  # $irods->remove_collection($irods_tmp_coll);
}

sub check_all_files : Test(2) {
  my $expt_name    = '2';
  my $device_id    = "GA10000";
  my $data_run_dir = "$data_path/$expt_name/$device_id";

  my $tmp_dir            = File::Temp->newdir(CLEANUP => 1)->dirname;
  # my $tmp_dir = File::Temp->newdir(DIR => '/nfs/users/nfs_k/kdj/dev/perl/npg_irods.git', CLEANUP => 0)->dirname;
  my $tmp_output_dir     = "$tmp_dir/output";
  my $tmp_basecalled_dir = "$tmp_dir/basecalled";
  my $tmp_run_dir        = "$tmp_basecalled_dir/$expt_name/$device_id";

  _do_publish_files($expt_name, $device_id, $data_run_dir,
                    $tmp_run_dir, $tmp_output_dir, $irods_tmp_coll);

  my $gridion_name = hostname;

  my $dest_coll = "$irods_tmp_coll/$gridion_name/$expt_name/$device_id";
  my $auditor = WTSI::NPG::HTS::ONT::GridIONTarAuditor->new
    (dest_collection => $dest_coll);

  my @f5_manifests = map { $_->manifest_path } @{$auditor->f5_manifests};
  is_deeply(\@f5_manifests, ["$dest_coll/2_GA10000_fast5_manifest.txt"],
            "Expected f5 manifests are present in $dest_coll") or
              diag explain \@f5_manifests;

  # diag explain $auditor->f5_manifests;
  # diag explain $auditor->fq_manifests;

  my @fq_manifests = map { $_->manifest_path } @{$auditor->fq_manifests};
  is_deeply(\@fq_manifests, ["$dest_coll/2_GA10000_fastq_manifest.txt"],
            "Expected fq manifests are present in $dest_coll") or
              diag explain \@fq_manifests;

  $auditor->zombat;
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
