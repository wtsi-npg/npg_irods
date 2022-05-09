package WTSI::NPG::HTS::PacBio::Sequel::JobExecute::JobTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
use File::Spec::Functions qw[catdir catfile];
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

use WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob;

my $pid           = $PID;
my $test_counter  = 0;
my $data_path     = 't/data/pacbio/sequel';
my $tmp_dir       = File::Temp->newdir->dirname;


sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob');
}

sub create_job : Test(3) {

  my @cmds;
  push @cmds, `which cp`;
 
  my @init_args  = (commands4jobs => \@cmds,
                    created_on    => DateTime->now(),
                    identifier    => 'my_job',
                    working_dir   => $tmp_dir,);

  my $job = WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob->new(@init_args);
  isa_ok($job, 'WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob');
  cmp_ok($job->execution_command, '=~', '^wr add', 'wr add job command created');

  $job->pre_execute;
  ok( -f $job->command_file_path, q{commands file exists} );
}

1;
