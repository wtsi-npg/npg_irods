#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use DateTime::Format::ISO8601;
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::iRODS;
use WTSI::NPG::OM::BioNano::Saphyr::RunPublisher;

our $VERSION = '';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.OM.BioNano.Saphyr = INFO, A1
log4perl.logger.WTSI.NPG.HTS = INFO, A1
log4perl.logger.WTSI.NPG.iRODS.Publisher = INFO, A1

# Errors from WTSI::NPG::iRODS are propagated in the code to callers
# in WTSI::NPG::HTS::PacBio, so we do not need to see them directly:

log4perl.logger.WTSI.NPG.iRODS = OFF, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

my $begin_date;
my $collection;
my $debug;
my $end_date;
my $log4perl_config;
my $verbose;

GetOptions('begin-date|begin_date=s'         => \$begin_date,
           'collection=s'                    => \$collection,
           'debug'                           => \$debug,
           'end-date|end_date=s'             => \$end_date,
           'help'                            => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'logconf=s'                       => \$log4perl_config,
           'verbose'                         => \$verbose);

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
$log->level($ALL);

my @init_args = (irods => WTSI::NPG::iRODS->new);
if ($collection) {
  push @init_args, dest_collection => $collection;
}

my $publisher = WTSI::NPG::OM::BioNano::Saphyr::RunPublisher->new(@init_args);

my @publish_args;
if ($begin_date) {
  push @publish_args,
    begin_date => DateTime::Format::ISO8601->parse_datetime($begin_date);
}
if ($end_date) {
  push @publish_args,
    end_date => DateTime::Format::ISO8601->parse_datetime($end_date);
}

my ($num_files, $num_published, $num_errors) =
  $publisher->publish_files(@publish_args);

if ($num_errors == 0) {
  $log->info("Processed $num_files, published $num_published ",
             "with $num_errors errors");
}
else {
  $log->logcroak("Processed $num_files, published $num_published ",
                 "with $num_errors errors");
}

__END__

=head1 NAME

npg_publish_saphyr_run

=head1 SYNOPSIS

npg_publish_saphyr_run [--collection <path>] [--begin-date <ISO datetime>]
  [--end-date <ISO datetime>] [--debug] [--verbose]
  [--logconf <path>]

 Options:
   --begin-date
   --begin_date      The earliest date for run completion to publish.
                     An ISO8601 date string. Optional, defaults to 7 days
                     earlier than the --end datetime.

                     e.g.
                         "2019-03-01 00:00:00"
                          2019-03-01T00:00:00
                          2019-03-01

   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/pacbio/.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --end-date
   --end_date        The latest date for run completion to publish.
                     An ISO8601 date string. Optional, to the current time.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Publishes one RawMoleculesBnx.gz per chip run, per flowcell to iRODS,
adds metadata and sets permissions.

The RawMoleculesBnx.gz file for each flowcell is copied to the local
host and published to iRODS. In addition, a JSON file containing
information from the database query relevant to that run is published
alongside the RawMoleculesBnx.gz file.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
