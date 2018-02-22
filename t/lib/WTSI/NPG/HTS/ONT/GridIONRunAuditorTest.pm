package WTSI::NPG::HTS::ONT::GridIONRunAuditorTest;

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

use WTSI::NPG::HTS::ONT::GridIONRunAuditor;
use WTSI::NPG::HTS::ONT::GridIONRunPublisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

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
    $irods->add_collection("GridIONRunAuditorTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub audit_files : Test(42) {
  my $expt_name    = '2';
  my $device_id    = "GA10000";
  my $data_run_dir = "$data_path/$expt_name/$device_id";

  my $tmp_dir            = File::Temp->newdir->dirname;
  my $tmp_output_dir     = "$tmp_dir/output";
  my $tmp_basecalled_dir = "$tmp_dir/basecalled";
  my $tmp_run_dir        = "$tmp_basecalled_dir/$expt_name/$device_id";

  _do_publish_files($expt_name, $device_id, $data_run_dir,
                    $tmp_run_dir, $tmp_output_dir, $irods_tmp_coll);

  my $gridion_name = hostname;

  my $auditor = WTSI::NPG::HTS::ONT::GridIONRunAuditor->new
    (dest_collection => $irods_tmp_coll,
     gridion_name    => $gridion_name,
     num_replicates  => 1,
     output_dir      => $tmp_output_dir,
     source_dir      => $tmp_run_dir);

  is($auditor->gridion_name, $gridion_name, "GridION name is '$gridion_name'");
  is($auditor->experiment_name, $expt_name, "Experiment name is '$expt_name'");
  is($auditor->device_id, $device_id, "Device ID is '$device_id'");

  # Check that it reports the good case
  my ($nfc, $npc, $nec) = $auditor->check_seq_cfg_files;
  cmp_ok($nfc, '==', 1, "Reports correct number of source cfg files");
  cmp_ok($npc, '==', 1, "Reports correct number of published cfg files");
  cmp_ok($nec, '==', 0, "Reports no errors for cfg files");

  my ($nfs, $nps, $nes) = $auditor->check_seq_summary_files;
  cmp_ok($nfs, '==', 2, "Reports correct number of source summary files");
  cmp_ok($nps, '==', 2, "Reports correct number of published summary files");
  cmp_ok($nes, '==', 0, "Reports no errors for summary files");

  my ($nfm, $npm, $nem) = $auditor->check_manifest_files;
  cmp_ok($nfm, '==', 2, "Reports correct number of source manifest files");
  cmp_ok($npm, '==', 2, "Reports correct number of published manifest files");
  cmp_ok($nem, '==', 0, "Reports no errors for manifest files");

  my ($nf5t, $np5t, $ne5t) = $auditor->check_f5_tar_files;
  cmp_ok($nf5t, '==', 7, "Reports correct number of f5 tar files");
  cmp_ok($np5t, '==', 7, "Reports correct number of published f5 tar files");
  cmp_ok($ne5t, '==', 0, "Reports no errors for f5 tar files");

  my ($nfqt, $npqt, $neqt) = $auditor->check_fq_tar_files;
  cmp_ok($nfqt, '==', 1, "Reports correct number of fq tar files");
  cmp_ok($npqt, '==', 1, "Reports correct number of published fq files");
  cmp_ok($neqt, '==', 0, "Reports no errors for fq tar files");

  my ($nf5, $np5, $ne5) = $auditor->check_f5_files;
  cmp_ok($nf5, '==', 40, "Reports correct number of f5 files");
  cmp_ok($np5, '==', 40, "Reports correct number of published f5 files");
  cmp_ok($neqt, '==', 0, "Reports no errors for f5 files");

  my ($nfq, $npq, $neq) = $auditor->check_fq_files;
  cmp_ok($nfq, '==', 2, "Reports correct number of fq files");
  cmp_ok($npq, '==', 2, "Reports correct number of published fq files");
  cmp_ok($neq, '==', 0, "Reports no errors for fq files");

  my ($num_files, $num_present, $num_errors) = $auditor->check_all_files;
  cmp_ok($num_files,   '==', 55, "Reports correct number of source files");
  cmp_ok($num_present, '==', 55, "Reports correct number of published files");
  cmp_ok($num_errors,  '==', 0, "Reports no errors for published files");

  # Check that it reports the bad cases: make some new primary files
  # that have not been published to simulate either a file being
  # missed (due to a bug) or new data being added e.g. the run
  # restarted.
  my $extra_cfg = "configuration2.cfg";
  my $extra_sum = "sequencing_summary_2.txt";
  my $extra_fq  = "fastq_2.fastq";
  my $extra_f5  =
    "GXB01030_20170907__GA10000_mux_scan_2_92904_read_999_ch_999_strand.fast5";

  copy("$data_run_dir/configuration.cfg", "$tmp_run_dir/$extra_cfg");
  copy("$data_run_dir/sequencing_summary_1.txt", "$tmp_run_dir/$extra_sum");
  copy("$data_run_dir/fastq_1.fastq", "$tmp_run_dir/$extra_fq");
  copy("$data_run_dir/reads/0/GXB01030_20170907__GA10000_mux_scan_2_92904_read_10_ch_256_strand.fast5",
       "$tmp_run_dir/reads/0/$extra_f5");

  my ($nfc_delta, $npc_delta, $nec_delta) = $auditor->check_seq_cfg_files;
  cmp_ok($nfc_delta, '==', 2,
         "Reports correct number of source cfg files");
  cmp_ok($npc_delta, '==', 1,
         "Reports correct number of published cfg files");
  cmp_ok($nec_delta, '==', 1,
         "Reports correct number of cfg file errors");

  my ($nfs_delta, $nps_delta, $nes_delta) = $auditor->check_seq_summary_files;
  cmp_ok($nfs_delta, '==', 3,
         "Reports correct number of source summary files");
  cmp_ok($nps_delta, '==', 2,
         "Reports correct number of published summary files");
  cmp_ok($nes_delta, '==', 1,
         "Reports correct number of summary file errors");

  my ($nf5_delta, $np5_delta, $ne5_delta) = $auditor->check_f5_files;
  cmp_ok($nf5_delta, '==', 41,
         "Reports correct number of f5 files");
  cmp_ok($np5_delta, '==', 40,
         "Reports correct number of published f5 files");
  cmp_ok($ne5_delta, '==', 1,
         "Reports correct number of summary file errors");

  my ($nfq_delta, $npq_delta, $neq_delta) = $auditor->check_fq_files;
  cmp_ok($nfq_delta, '==', 3,
         "Reports correct number of fq files");
  cmp_ok($npq_delta, '==', 2,
         "Reports correct number of published fq files");
  cmp_ok($neq_delta, '==', 1,
         "Reports correct number of fq file errors");

  my ($num_files_delta, $num_present_delta, $num_errors_delta) =
    $auditor->check_all_files;
  cmp_ok($num_files_delta,   '==', 59,
         "Reports correct number of source files");
  cmp_ok($num_present_delta, '==', 55,
         "Reports correct number of published files");
  cmp_ok($num_errors_delta, '==', 4,
         "Reports correct number of total file errors");
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
