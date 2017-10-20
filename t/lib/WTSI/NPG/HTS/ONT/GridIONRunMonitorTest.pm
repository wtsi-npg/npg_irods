package WTSI::NPG::HTS::ONT::GridIONRunMonitorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use Log::Log4perl;
use File::Spec::Functions qw[catdir curdir rel2abs splitdir];
use File::Path qw[make_path];
use File::Temp;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::ONT::GridIONRunMonitor;
use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path = 't/data/ont/gridion';

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

sub start : Test(1) {
  my $tmp_dir = File::Temp->newdir->dirname;
  make_path $tmp_dir;

  my $monitor = WTSI::NPG::HTS::ONT::GridIONRunMonitor->new
    (dest_collection => $irods_tmp_coll,
     session_timeout => 20,
     source_dir      => $tmp_dir);

  local $SIG{ALRM} = sub { $monitor->monitor(0) };
  alarm 10;

  my $num_errors = $monitor->start;
  cmp_ok($num_errors, '==', 0, 'Monitor exited cleanly');
}

sub watch_history : Test(1) {
  my $tmp_dir = File::Temp->newdir->dirname;
  my @tmp_dirs = splitdir($tmp_dir);
  make_path(catdir(@tmp_dirs, "expt1", "GA10000", "reads", "0"));
  make_path(catdir(@tmp_dirs, "expt1", "GA10000", "reads", "1"));
  make_path(catdir(@tmp_dirs, "expt2", "GA20000", "reads"));

  my $monitor = WTSI::NPG::HTS::ONT::GridIONRunMonitor->new
    (dest_collection => $irods_tmp_coll,
     session_timeout => 20,
     source_dir      => $tmp_dir);

  local $SIG{ALRM} = sub { $monitor->monitor(0) };
  alarm 10;

  $monitor->start;

  # Simulate adding further directories under an existing one. This
  # should not cause the expt2 directory to be added to the watch
  # history multiple times
  foreach my $i (0 .. 9) {
    make_path catdir(@tmp_dirs, "expt2", "GA20000", "reads", $i);
  }

  my @expected = ($tmp_dir,
                  "$tmp_dir/expt1",
                  "$tmp_dir/expt2");
  my $watch_history = $monitor->watch_history;
  is_deeply($watch_history, \@expected,
            'Watch history is correct for pre-existing directories') or
              diag explain $watch_history;
}

1;
