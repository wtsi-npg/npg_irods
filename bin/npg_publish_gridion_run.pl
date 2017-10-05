#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use English qw[-no_match_vars];
use File::Spec::Functions qw[rel2abs];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::GridIONRunPublisher;

our $VERSION = '';

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $arch_capacity   = 10_000;
my $arch_duration   = 60 * 60 * 6;
my $arch_timeout    = 60 * 5;
my $collection;
my $debug;
my $log4perl_config;
my $session_timeout = 60 * 20;
my $source_dir;
my $verbose;
##use critic

GetOptions('collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                         => \$log4perl_config,
           'session-timeout|session_timeout=s' => \$session_timeout,
           'source_dir|source-dir=s'           => \$source_dir,
           'tar-capacity|tar_capacity=i'       => \$arch_capacity,
           'tar-duration|tar_duration=i'       => \$arch_duration,
           'tar-timeout|tar_timeout=i'         => \$arch_timeout,
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

$source_dir or
  $log->logcroak('A source_dir argument is required');
-d $source_dir or
  $log->logcroak('Invalid source_dir argument: not a directory');

$source_dir = rel2abs($source_dir);

my $publisher = WTSI::NPG::HTS::ONT::GridIONRunPublisher->new
  (arch_capacity   => $arch_capacity,
   arch_duration   => $arch_duration,
   arch_timeout    => $arch_timeout,
   dest_collection => $collection,
   source_dir      => $source_dir,
   session_timeout => 200)->publish_files;

__END__

=head1 NAME

npg_publish_gridion_run

=head1 SYNOPSIS

npg_publish_gridion_run --collection <path> [--debug] [--logconf <path>]
  --source-dir <path> [--tar-capacity <n>] [--tar-duration <n>]
  [--tar-timeout <n>] [--verbose]

 Options:
   --collection      The destination collection in iRODS.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --source-dir
   --source_dir      The instrument device directory path to watch.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --session-timeout
   --session_timeout The number of seconds idle time after which a multi-file
                     tar session will be closed. Optional, defaults to 60 * 20
                     seconds.
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
