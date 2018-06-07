#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::GridIONTarAuditor;

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
my $log4perl_config;
my $tmpdir = '/tmp';
my $verbose;

GetOptions('collection=s' => \$collection,
           'debug'        => \$debug,
           'help'         => sub { pod2usage(-verbose => 2,
                                             -exitval => 0) },
           'logconf=s'    => \$log4perl_config,
           'tmpdir=s'     => \$tmpdir,
           'verbose'      => \$verbose);

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

my $auditor = WTSI::NPG::HTS::ONT::GridIONTarAuditor->new
  (dest_collection => $collection,
   tmpdir          => $tmpdir);

my ($num_files, $num_published, $num_errors) = $auditor->check_all_files;

my $msg = sprintf q[Checked %d files published to '%s' with %d errors],
  $num_files, $auditor->dest_collection, $num_errors;

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

npg_audit_gridion_tar

=head1 SYNOPSIS

npg_audit_gridion_tar --collection <path> [--debug]
  [--logconf <path>] [--verbose]

 Options:
   --collection      The root collection in iRODS for GridION data. e.g.
                     '/seq/ont/gridion'.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --tmpdir          Set the temporary directory where tar files from
                     iRODS are expanded - may require a lot of space.
                     Optional, defaults to /tmp.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Checks that the tar manifests and tar files of a single GridION run
(the results of a single flowcell) are in iRODS by comparing the contents
of the manifest with the contents of the tar files in iRODS collection into
which the data were published.

The following are checked:

 - Some tar manifest files are in iRODS.
 - The tar files described in the manifest(s) and in iRODS.
 - The complement of tarred files and their checksums correspond to those
   described in manifest(s).

If all files are correct this script exits with success, otherwise it
exits with an error.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
