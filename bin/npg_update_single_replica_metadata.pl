#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use DateTime::Format::ISO8601;
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::Data::SingleReplicaMetaUpdater;
use WTSI::NPG::DriRODS;
use WTSI::NPG::iRODS;

our $VERSION = '';
our $DEFAULT_COLLECTION = '/seq';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.Data.SingleReplicaMetaUpdater = INFO, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

my $begin_date;
my $debug;
my $dry_run;
my $end_date;
my $log4perl_config;
my $limit = 0;
my $verbose;

GetOptions('begin-date|begin_date=s' => \$begin_date,
           'debug'                   => \$debug,
           'dry-run|dry_run!'        => \$dry_run,
           'end-date|end_date=s'     => \$end_date,
           'help'                    => sub { pod2usage(-verbose => 2,
                                                        -exitval => 0) },
           'logconf=s'               => \$log4perl_config,
           'limit=i'                 => \$limit,
           'verbose'                 => \$verbose);

# Process CLI arguments
if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  if ($verbose and not $debug) {
    Log::Log4perl::init(\$verbose_config);
  }
  else {
    my $level = $debug ? $DEBUG : $WARN;
    Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
        level  => $level,
        utf8   => 1});
  }
}

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

my $irods;
if ($dry_run) {
  $irods = WTSI::NPG::DriRODS->new;
}
else {
  $irods = WTSI::NPG::iRODS->new;
}

my $updater = WTSI::NPG::Data::SingleReplicaMetaUpdater->new(irods => $irods);
my $grace = DateTime::Duration->new(years => $updater->default_grace_period);
my $grace_threshold = DateTime->now->subtract($grace);

my $begin;
if (defined $begin_date) {
  $begin = DateTime::Format::ISO8601->parse_datetime($begin_date);
}
else {
  $begin = DateTime->from_epoch(epoch => 0);
}

my $end;
if (defined $end_date) {
  $end = DateTime::Format::ISO8601->parse_datetime($end_date);
}
else {
  $end = $grace_threshold->clone->subtract(DateTime::Duration->new(days => 1))
}

if (not DateTime->compare($end, $grace_threshold) < 0) {
  $log->logcroak(sprintf q[The end date %s is too recent; it must be ] .
                         q[earlier than the grace period threshold %s],
                         $end->iso8601, $grace_threshold->iso8601);
}
if (not DateTime->compare($begin, $grace_threshold) < 0) {
  $log->logcroak(sprintf q[The begin date %s is too recent; it must be ] .
                         q[earlier than the grace period threshold %s],
                         $begin->iso8601, $grace_threshold->iso8601);
}
if (not DateTime->compare($begin, $end) < 0) {
  $log->logcroak(sprintf q[The begin date %s is too recent; it must be ] .
                         q[earlier than the end date %s],
                         $begin->iso8601, $end->iso8601);
}

if ($limit) {
  $limit =~ m{^\d+$}msx or
      $log->logcroak(sprintf q[The limit must be a non-negative ] .
                             q[integer; %s is invalid],
                             $limit);
  $limit = int $limit;
}

my $limit_msg = $limit ? ", limited to $limit objects" : q[];
$log->info(sprintf q[Updating objects with creation dates between %s ] .
                   q[and %s%s], $begin, $end, $limit_msg);

my ($num_objects, $num_processed, $num_errors) =
    $updater->update_single_replica_metadata(begin_date => $begin,
                                             end_date   => $end,
                                             limit      => $limit);

my $msg = sprintf q[Found %d data objects, processed %d with %d errors],
                  $num_objects, $num_processed, $num_errors;
if ($num_errors != 0) {
  $log->logcroak($msg);
}

$log->info($msg);

__END__

=head1 NAME

npg_update_single_replica_metadata

=head1 SYNOPSIS

npg_update_single_replica_metadata [--begin-date YYYY-MM-DD] [--debug]
[--end-date YYYY-MM-DD] [--help] [--limit <n>] [--logconf file] [--verbose]

 Options:

  --begin-date
  --begin_date  Earliest creation date of data objects to consider for
                update. Optional, defaults to the epoch.
  --debug       Enable debug level logging. Optional, defaults to false.
  --dry-run
  --dry_run     Enable dry-run mode. Find eligible data objects and report
                them, but do not update them. Optional, defaults to false.
  --end-date
  --end_date    Latest creation date of data objects to consider for update.
                Optional, defaults to 1 day earlier than the grace period.
  --help        Display help.
  --limit       Limit the number of updates done to this number. Optional.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.

  The date arguments may be in any format accepted by
  DateTime::Format::ISO8601 e.g.

    YYYYMMDD
    YYYY-MM-DD
    YYYYMMDDThhmm
    YYYY-MM-DDThh:mm

=head1 DESCRIPTION

This script finds data objects that are eligible for migration to a
single-replica resource and marks them with metadata that will instruct
the iRODS server to do that.

Candidate data objects are those that have metadata dcterms:created between
the begin-date and end date and are not already marked for migration. The
end date may not be more recent than the current date, minus the grace period.

In dry run mode, the proposed metadata changes will be written as INFO
notices to the log.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2022 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
