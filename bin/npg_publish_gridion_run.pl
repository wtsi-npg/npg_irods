#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use English qw[-no_match_vars];
use File::Spec::Functions qw[rel2abs splitdir];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::GridIONRunPublisher;

our $VERSION = '';

my $default_log_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

# Errors from WTSI::NPG::iRODS are propagated in the code to callers
# in WTSI::NPG::HTS::Illumina, so we do not need to see them directly:

log4perl.logger.WTSI.NPG.iRODS = OFF, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $arch_capacity   = 10_000;
my $arch_duration   = 60 * 60 * 6;
my $arch_timeout    = 60 * 5;
my $collection;
my $debug;
my $old_style_tar;
my $gridion_name;
my $log4perl_config;
my $output_dir;
my $session_timeout = 60 * 20;
my $single_server;
my $source_dir;
my $tmpdir = '/tmp';
my $verbose;
##use critic

GetOptions('collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'old-style-tar'                     => \$old_style_tar,
           'gridion-name|gridion_name=s'       => \$gridion_name,
           'logconf=s'                         => \$log4perl_config,
           'output-dir|output_dir=s'           => \$output_dir,
           'session-timeout|session_timeout=s' => \$session_timeout,
           'single-server|single_server'       => \$single_server,
           'source-dir|source_dir=s'           => \$source_dir,
           'tar-capacity|tar_capacity=i'       => \$arch_capacity,
           'tar-duration|tar_duration=i'       => \$arch_duration,
           'tar-timeout|tar_timeout=i'         => \$arch_timeout,
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

  # Muffle the iRODS logger
  Log::Log4perl->get_logger('WTSI.NPG.iRODS')->level($OFF);
}

my $log = Log::Log4perl->get_logger('main');
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

$collection or
  $log->logcroak('A collection argument is required');

$gridion_name or
  $log->logcroak('A gridion-name argument is required');

$output_dir or
  $log->logcroak('An output-dir argument is required');
-d $output_dir or
  $log->logcroak('Invalid output_dir argument: not a directory');
$output_dir = rel2abs($output_dir);

$source_dir or
  $log->logcroak('A source_dir argument is required');
-d $source_dir or
  $log->logcroak('Invalid source_dir argument: not a directory');

$source_dir = rel2abs($source_dir);

my @elts = splitdir($source_dir);

my $device_id = pop @elts;
my $expt_name = pop @elts;

my $publisher = WTSI::NPG::HTS::ONT::GridIONRunPublisher->new
  (arch_capacity     => $arch_capacity,
   arch_duration     => $arch_duration,
   arch_timeout      => $arch_timeout,
   dest_collection   => $collection,
   device_id         => $device_id,
   extra_tar_context => $old_style_tar ? 0 : 1,
   experiment_name   => $expt_name,
   gridion_name      => $gridion_name,
   output_dir        => $output_dir,
   single_server     => $single_server,
   source_dir        => $source_dir,
   session_timeout   => $session_timeout,
   tmpdir            => $tmpdir)->publish_files;

__END__

=head1 NAME

npg_publish_gridion_run

=head1 SYNOPSIS

npg_publish_gridion_run --collection <path> [--debug] [--logconf <path>]
  --output-dir <path> [--single-server] --source-dir <path>
  [--tar-capacity <n>] [--tar-duration <n>]
  [--tar-timeout <n>] [--verbose]

 Options:
   --collection      The destination collection in iRODS.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --source-dir
   --source_dir      The instrument device directory path to watch.
   --gridion-name    The GridION instrument hostname. e.g. GXB01030. Optional,
                     defaults to the current hostname. This option MUST be used
                     if publishing from a host other than a GridION.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --output-dir
   --output_dir      A writable local directory where log files and
                     file manifests will be written.
   --session-timeout
   --session_timeout The number of seconds idle time after which a multi-file
                     tar session will be closed. Optional, defaults to 60 * 20
                     seconds.
   --single-server
   --single_server   Connect to only one iRODS server.
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
   --tmpdir          Temporary file directory. Optional, defaults to /tmp.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

This is a script to publish completed GridION runs to iRODS on and ad hoc
basis. The publishing process is normally conducted automatically by a
run monitor service on the GridION itself. However, there are occasions
when this script is useful e.g. when publishing from a copy of the data
that has been mirrored to a staging area with rsync or in order to fill
in a missing file that was not published initially, due to e.g. a network
outage.

The source-dir argument must be the full path to a device directory:

  --source-dir /nfs/sf-nfs-01-01/ONT/gridion/by_expt/4/GA30000

The output-dir argument must be the full path to the directory where the
manifest files to date for that device are located:

  --output-dir /nfs/sf-nfs-01-01/ONT/gridion/npg/4/GA30000

The collection argument must be a full apth in iRODS, sans GridION name,
experiment name and device ID:

  --collection /seq/ont/gridion

The GridION name is optional only when publishing from a GridION, otherwise
it must be provided. It must be the GridION on which the run was carried
out:

  --gridion-name GXB01030

The optional old-style-tar option is for use only when re-publishing older
data whose tar files did not contain the experiment name and device ID of
the run in the tarred data i.e.

  reads/0/
  reads/1/
  fastq_1.fastq

rather than the newer convention of:

  1/GA10000/reads/0
  1/GA10000/reads/1
  1/GA10000/fastq_1.fastq


To publish a completed run immediately, rather than waiting for the
session timeout, it is safe to provide a very short session timeout
e.g.  10 seconds.


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
