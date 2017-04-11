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

use WTSI::NPG::HTS::ONT::MinIONRunMonitor;

our $VERSION = '';

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $arch_capacity   = 10_000;
my $arch_timeout    = 60 * 5;
my $collection;
my $debug;
my $log4perl_config;
my $session_timeout = 60 * 20;
my $staging_path;
my $verbose;
#use critic

GetOptions('collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                         => \$log4perl_config,
           'session-timeout|session_timeout=s' => \$session_timeout,
           'staging_path|staging-path=s'       => \$staging_path,
           'tar_capacity|tar-capacity=i'       => \$arch_capacity,
           'tar_timeout|tar-timeout=i'         => \$arch_timeout,
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


my $monitor = WTSI::NPG::HTS::ONT::MinIONRunMonitor->new
  (arch_capacity   => $arch_capacity,
   arch_timeout    => $arch_timeout,
   dest_collection => $collection,
   session_timeout => $session_timeout,
   staging_path    => $staging_path);

my $num_errors = $monitor->start;
my $exit_code  = $num_errors == 0 ? 0 : 4;

exit $exit_code;

__END__

=head1 NAME

npg_minion_run_monitor

=head1 SYNOPSIS

npg_minion_run_monitor [--debug] [--logconf <path>] --staging-path <path>
  [--tar-capacity <n>] [--tar-timeout <n>] [--verbose]

 Options:
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --session-timeout
   --session_timeout The number of seconds idle time after which a multi-file
                     tar session will be closed. Optional, defaults to 60 * 20
                     seconds.
   --staging-path
   --staging_path    The data staging path to watch.
   --tar-capacity
   --tar_capacity    The number of read files to be archived per tar file.
                     Optional, defaults to 10,000.
   --tar-timeout
   --tar_timeout     The number of seconds idle time after which a tar file
                     open for writing, will be closed. even if it has not
                     reached capacity. Optional, defaults to 60 * 5 seconds.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
