#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;
use Sys::Hostname;

use WTSI::NPG::HTS::ONT::GridIONRunAuditor;

our $VERSION = '';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.ONT = INFO, A1
log4perl.logger.WTSI.NPG.HTS = INFO, A1
log4perl.logger.WTSI.NPG.iRODS.Publisher = INFO, A1

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

my $collection;
my $debug;
my $gridion_name = hostname;
my $log4perl_config;
my $num_replicates = 1;
my $output_dir;
my $source_dir;
my $verbose;

GetOptions('collection=s'                  => \$collection,
           'debug'                         => \$debug,
           'gridion-name|gridion_name=s'   => \$gridion_name,
           'help'                          => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                     => \$log4perl_config,
           'num-replicates|num_replicates' => \$num_replicates,
           'output-dir|output_dir=s'       => \$output_dir,
           'source-dir|source_dir=s'       => \$source_dir,
           'verbose'                       => \$verbose);

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
  Log::Log4perl->get_logger('main')->info
      ("Using log config file '$log4perl_config'");
}
elsif ($verbose and not $debug) {
  Log::Log4perl::init(\$verbose_config);
}
else {
  my $level = $debug ? $DEBUG : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
  Log::Log4perl->get_logger('WTSI.NPG.iRODS')->level($OFF);
}

$collection or
  pod2usage(-msg     => 'A --collection argument is required',
            -exitval => 2);
$output_dir or
  pod2usage(-msg     => 'An --output-dir argument is required',
            -exitval => 2);
-d $output_dir or
  pod2usage(-msg     => 'Invalid --output-dir argument: not a directory',
            -exitval => 2);
$source_dir or
  pod2usage(-msg     => 'A --source-dir argument is required',
            -exitval => 2);
-d $source_dir or
  pod2usage(-msg     => 'Invalid --source-dir argument: not a directory',
            -exitval => 2);

my $auditor = WTSI::NPG::HTS::ONT::GridIONRunAuditor->new
  (dest_collection => $collection,
   gridion_name    => $gridion_name,
   num_replicates  => $num_replicates,
   output_dir      => $output_dir,
   source_dir      => $source_dir);

my ($num_files, $num_published, $num_errors) = $auditor->check_all_files;

my $msg = sprintf q[Checked %d local files, %d published from ] .
                  q['%s' to '%s' with %d errors],
  $num_files, $num_published, $source_dir, $auditor->run_collection,
  $num_errors;

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

if ($num_errors == 0) {
  $log->info($msg);
}
else {
  $log->logcroak($msg);
}

__END__

=head1 NAME

npg_audit_gridion_run

=head1 SYNOPSIS

npg_audit_gridion_run --collection <path> [--debug]
  [--gridion-name <name>] [--logconf <path>] [--num-replicates <n>]
  --output-dir <path> --source-dir <path>  [--verbose]

 Options:
   --collection      The root collection in iRODS for GridION data. e.g.
                     '/seq/ont/gridion'.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --gridion-name
   --gridion_name    The name of the GridION host used to publish the
                     data. Optional, defaults to the current hostname.
   --num-replicates
   --num_replicates  The minimum number of valid replicates expected in
                     iRODS. Optional, defaults to 1.
   --output-dir
   --output_dir      The GridION publisher output directory containing
                     the tar manifests for the run e.g.
                     '/data/npg/5/GA10000'.
   --source-dir
   --source_dir      The device directory containing the local raw
                     data e.g. '/data/basecalled/5/GA10000'.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Checks that the files of a single GridION run (the results of a single
flowcell) are in iRODS by comparing the contents of the local run
directory with the contents of the iRODS collection into which the
data were published.

The following are checked:

 - Local configuration.cfg files are in iRODS.
 - Local sequencing_summary_n.txt files are in iRODS.
 - Local tar manifest files are in iRODS.
 - Local fastq files are mapped to tar files in iRODS by a tar manifest.
 - Tar files described in tar manifests are in iRODS.

If all files are present this script exits with success, otherwise it
exits with an error.

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
