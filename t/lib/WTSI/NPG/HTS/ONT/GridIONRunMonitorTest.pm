package WTSI::NPG::HTS::ONT::GridIONRunMonitorTest;

use strict;
use warnings;

use File::Spec::Functions qw[catdir curdir rel2abs splitdir];
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::ONT::GridIONRunMonitor;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = 't/data/ont/gridion';

sub start : Test(1) {
  my $monitor = WTSI::NPG::HTS::ONT::GridIONRunMonitor->new
    (dest_collection => '/test',
     staging_path    => $data_path);

  local $SIG{ALRM} = sub { $monitor->monitor(0) };
  alarm 10;

  my $num_errors = $monitor->start;
  cmp_ok($num_errors, '==', 0, 'Monitor exited cleanly');
}

sub watch_history : Test(1) {
  my $monitor = WTSI::NPG::HTS::ONT::GridIONRunMonitor->new
    (dest_collection => '/test',
     staging_path    => $data_path);

  local $SIG{ALRM} = sub { $monitor->monitor(0) };
  alarm 10;

  $monitor->start;

  my @staging_dir = (splitdir(rel2abs(curdir)), splitdir($data_path));
  my @expected = (catdir(@staging_dir),
                  catdir(@staging_dir, '2'),
                  catdir(@staging_dir, '3'));
  my $watch_history = $monitor->watch_history;
  is_deeply($watch_history, \@expected,
            'Watch history is correct for pre-existing directories') or
              diag explain $watch_history;
}

1;
