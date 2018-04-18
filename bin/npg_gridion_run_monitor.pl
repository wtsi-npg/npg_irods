#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Carp;
use Data::Dump qw[pp];
use File::Spec::Functions qw[abs2rel splitpath];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::GridIONRunMonitor;

our $VERSION = '';

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $arch_capacity   = 10_000;
my $arch_duration   = 60 * 60 * 6;
my $arch_timeout    = 60 * 5;
my $collection;
my $debug;
my $log4perl_config;
my $output_dir;
my $poll_interval   = 60;
my $quiet_interval  = 60 * 60 * 24;
my $session_timeout = 60 * 20;
my $single_server;
my $staging_dir;
my $tmpdir          = '/tmp';
my $verbose;
#use critic

GetOptions('collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                         => \$log4perl_config,
           'output-dir|output_dir=s'           => \$output_dir,
           'poll-interval|poll_interval=i'     => \$poll_interval,
           'quiet-interval|quiet_interval=i'   => \$quiet_interval,
           'session-timeout|session_timeout=s' => \$session_timeout,
           'single-server|single_server'       => \$single_server,
           'staging-dir|staging_dir=s'         => \$staging_dir,
           'tar_capacity|tar-capacity=i'       => \$arch_capacity,
           'tar-duration|tar_duration=i'       => \$arch_duration,
           'tar_timeout|tar-timeout=i'         => \$arch_timeout,
           'tmpdir=s'                          => \$tmpdir,
           'verbose'                           => \$verbose);

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

my $log = Log::Log4perl->get_logger('main');
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

$collection or
  $log->logcroak('A collection argument is required');

$output_dir or
  $log->logcroak('An output-dir argument is required');

my $monitor = WTSI::NPG::HTS::ONT::GridIONRunMonitor->new
  (arch_capacity   => $arch_capacity,
   arch_duration   => $arch_duration,
   arch_timeout    => $arch_timeout,
   dest_collection => $collection,
   output_dir      => $output_dir,
   poll_interval   => $poll_interval,
   quiet_interval  => $quiet_interval,
   session_timeout => $session_timeout,
   single_server   => $single_server,
   source_dir      => $staging_dir,
   tmpdir          => $tmpdir);

# Ensure a clean exit
local $SIG{INT}  = sub { $monitor->monitor(0) };
local $SIG{TERM} = sub { $monitor->monitor(0) };

my $num_errors = $monitor->start;
my $exit_code  = $num_errors == 0 ? 0 : 4;

$log->info('In progress: ', pp($monitor->devices_active));
$log->info('Completed: ', pp($monitor->devices_complete));

exit $exit_code;

__END__

=head1 NAME

npg_gridion_run_monitor

=head1 SYNOPSIS

npg_gridion_run_monitor --collection <path> [--debug] [--logconf <path>]
  --output-dir <path> [--poll-interval <n>] [--quiet-interval <n>]
  [--single-server] --staging-dir <path>
  [--tar-capacity <n>] [--tar-timeout <n>]
  [--tmpdir <path>] [--verbose]

 Options:
   --collection      The root iRODS collection in which to write data,
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --output-dir
   --output_dir      A writable local directory where log files and
                     file manifests will be written.
   --poll-interval
   --poll_interval   The number of seconds between polls to the filesystem
                     to check for new experiment and device directories.
                     Optional, defauls to 60 seconds.
   --quiet-interval
   --quiet_interval  The number of seconds after a publisher has successfully
                     completed during which time it will not be restarted
                     if its device directory remains in the staging directory.
                     Optional, defaults to 60 * 60 * 24 seconds.
   --session-timeout
   --session_timeout The number of seconds idle time after which a multi-file
                     tar session will be closed. Optional, defaults to 60 * 20
                     seconds.
   --single-server
   --single_server   Connect to only one iRODS server.
   --staging-dir
   --staging_dir     The data staging directory path to watch.
   --tar-capacity
   --tar_capacity    The number of read files to be archived per tar file.
                     Optional, defaults to 10,000.
   --tar-duration
   --tar_duration    The maximum number of seconds a tar file may be open for
                     writing. Optional, defaults to 60 * 60 * 6 seconds.
   --tar-timeout
   --tar_timeout     The number of seconds idle time after which a tar file
                     open for writing, will be closed. even if it has not
                     reached capacity. Optional, defaults to 60 * 5 seconds.
   --tmpdir          The Unix TMPDIR where publishers will create their
                     temporary working directories. Optional, defaults to
                     /tmp.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Polls a staging area for new GridION experiment result
directories. Launches a WTSI::NPG::HTS::ONT::GridIONRunPublisher for
each existing device directory and for any new device directory
created.

For full documentation see WTSI::NPG::HTS::ONT::GridIONRunMonitor.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017, 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
