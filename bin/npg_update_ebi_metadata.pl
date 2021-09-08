#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use DateTime::Format::ISO8601;
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::DataSub::MetaUpdater;
use WTSI::NPG::DataSub::SubtrackClient;
use WTSI::NPG::DriRODS;
use WTSI::NPG::iRODS;

our $VERSION = '';
our $DEFAULT_COLLECTION = '/seq';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.DataSub.MetaUpdater = INFO, A1
log4perl.logger.WTSI.NPG.DataSub.SubtrackClient = INFO, A1
log4perl.logger.WTSI.NPG.iRODS.DataObject = INFO, A1
log4perl.logger.WTSI.NPG.DriRODS = INFO, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

my $begin_date;
my $collections = [];
my $debug;
my $dry_run = 1;
my $end_date;
my $log4perl_config;
my $verbose;

GetOptions('begin-date|begin_date=s'   => \$begin_date,
           'debug'                     => \$debug,
           'collection=s@'             => \$collections,
           'dry-run|dry_run!'          => \$dry_run,
           'end-date|end_date=s'       => \$end_date,
           'help'                      => sub { pod2usage(-verbose => 2,
                                                          -exitval => 0) },
           'logconf=s'                 => \$log4perl_config,
           'verbose'                   => \$verbose);

# Process CLI arguments
if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  if ($verbose and ($dry_run and not $debug)) {
    Log::Log4perl::init(\$verbose_config);
  }
  elsif ($debug) {
    Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                              level  => $DEBUG,
                              utf8   => 1})
  }
  else {
    Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                              level  => $ERROR,
                              utf8   => 1})
  }
}

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

@{$collections} or $collections = [$DEFAULT_COLLECTION];

my $irods;
if ($dry_run) {
  $irods = WTSI::NPG::DriRODS->new;
}
else {
  $irods = WTSI::NPG::iRODS->new;
}

my $subtrack = WTSI::NPG::DataSub::SubtrackClient->new;


my @query_args;
if (defined $begin_date) {
  push @query_args, begin_date =>
    DateTime::Format::ISO8601->parse_datetime($begin_date);
}
if (defined $end_date) {
  push @query_args, end_date =>
    DateTime::Format::ISO8601->parse_datetime($end_date);
}
else {
  push @query_args, end_date => DateTime->now;
}

my @submitted_files = $subtrack->query_submitted_files(@query_args);

$log->info('Processing ', scalar @submitted_files, ' submitted files');

# Update metadata
my $num_updated = 0;

if (@submitted_files) {
  my $meta_updater = WTSI::NPG::DataSub::MetaUpdater->new(irods => $irods);
  $num_updated =
    $meta_updater->update_submission_metadata($collections, \@submitted_files);
}

$log->info("Updated metadata on $num_updated submitted files");


__END__

=head1 NAME

npg_update_ebi_metadata

=head1 SYNOPSIS

npg_update_ebi_metadata [--begin-date YYYY-MM-DD] [--collection path]
  [--debug] [--dry-run] [--end-date YYYY-MM-DD] [--help] [--logconf file]
  [--verbose]

 Options:

  --begin-date
  --begin_date  Submission update date range to query, beginning. Optional,
                defaults to 14 days prior to date given as the --end-date
                argument.
  --collection  The iRODS collections in which to work - can be given multiple 
                times. Optional, defaults to '/seq'.
  --debug       Enable debug level logging. Optional, defaults to false.
  --dry-run
  --dry_run     Enable dry-run mode. Propose metadata changes, but do not
                perform them. Optional, defaults to true.
  --end-date
  --end_date    Submission update date range to query, end. Optional,
                defaults to the curent time.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.

  The date arguments may be in any format accepted by
  DateTime::Format::ISO8601 e.g.

    YYYYMMDD
    YYYY-MM-DD
    YYYYMMDDThhmm
    YYYY-MM-DDThh:mm

=head1 DESCRIPTION

This script updates EBI sumbission metadata on any data object in
iRODS whose submission to the EBI to tracked by the 'subtrack'
database. Data objects whose submission status has changed during a
user-specified date window will have their metadata updated, if
necessary.

The attributes added or updated are:

  ebi_run_acc
  ebi_sub_acc
  ebi_sub_date
  ebi_sub_md5

If metadata changes, the existing values are superseded (and moved
into a history attribute). Multiple AVUs with the same key are not
retained.

In dry run mode, the proposed metadata changes will be written as INFO
notices to the log.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
