#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[uniq];
use Log::Log4perl::Level;
use POSIX;
use Pod::Usage;
use Readonly;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::DriRODS;
use WTSI::NPG::HTS::Illumina::MetaUpdater;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::icommands qw[iquest];

our $VERSION = '';

Readonly::Scalar my $DEFAULT_ZONE => 'seq';
Readonly::Scalar my $THOUSAND     => 1000;
Readonly::Scalar my $SEC_PER_MIN  => 60;

sub _read_composition_paths_stdin {
  my ($log, $stdio) = @_;

  my @composition_paths;
  if ($stdio) {
    binmode \*STDIN, 'encoding(UTF-8)';

    while (my $line = <>) {
      chomp $line;
      push @composition_paths, $line;
    }

    $log->info('Read ', scalar @composition_paths,
               ' iRODS composition file paths from STDIN');
  }

  return @composition_paths;
}

sub _filter_composition_paths_irods {
  my ($logger, $zone, $collections, $id_run) = @_;

  my @filtered;

  foreach my $collection (@{$collections}) {
    $logger->debug('Working on collection ', $collection);
    my @paths = _find_composition_paths_irods($logger, $zone, $collection);

    my @filter;
    foreach my $id (@{$id_run}) {
      my $d = int($id / $THOUSAND);
      push @filter, "\Q$collection/$d/$id/\E", "\Q$collection/$id/\E";
    }

    my $pattern = sprintf q[^(%s)], join q[|], @filter;
    $logger->debug('Filtering with ', $pattern);
    my $re = qr{$pattern}msx;
    push @filtered, grep { m{$re}msx } @paths;
  }

  $logger->debug('Found paths: ', pp(\@filtered));

  return @filtered;
}

sub _find_composition_paths_irods {
  my ($logger, $zone, $collection) = @_;

  my $query = sprintf q[select COLL_NAME, DATA_NAME where ] .
                      q[COLL_NAME like '%s/%%' ]          .
                      q[and DATA_NAME like '%%.composition.json'], $collection;

  $logger->debug('Running ', $query);
  my $start_time = time;

  my @paths = iquest('-z', $zone, q[%s/%s], $query);
  my $num_paths = scalar @paths;
  my $duration  = time - $start_time;
  my $num_min = floor($duration / $SEC_PER_MIN);
  my $num_sec = $duration % $SEC_PER_MIN;
  $logger->info("Found $num_paths composition files in $num_min min ",
                "$num_sec sec");

  return @paths;
}


my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.Illumina.AlnDataObject = INFO, A1
log4perl.logger.WTSI.NPG.HTS.Illumina.MetaUpdater = INFO, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

my $collections = [];
my $debug;
my $driver_type;
my $dry_run = 1;
my @id_run;
my $log4perl_config;
my $max_id_run;
my $min_id_run;
my $stdio;
my $verbose;
my $zone = $DEFAULT_ZONE;

GetOptions('collection=s@'             => \$collections,
           'debug'                     => \$debug,
           'driver-type|driver_type=s' => \$driver_type,
           'dry-run|dry_run!'          => \$dry_run,
           'help'                      => sub { pod2usage(-verbose => 2,
                                                          -exitval => 0) },
           'logconf=s'                 => \$log4perl_config,
           'max_id_run|max-id-run=i'   => \$max_id_run,
           'min_id_run|min-id-run=i'   => \$min_id_run,
           'id_run|id-run=i'           => \@id_run,
           'verbose'                   => \$verbose,
           'zone=s'                    => \$zone,
           q[]                         => \$stdio);

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

if ((defined $max_id_run and not defined $min_id_run) ||
    (defined $min_id_run and not defined $max_id_run)) {
  my $msg = 'When used, the --max-run or --min-run options must be ' .
            'used together';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}
if ((defined $max_id_run and defined $min_id_run) and
    ($max_id_run < $min_id_run)) {
  my $msg = "The --max-run value ($max_id_run) must be >= ".
            "the --min-run value ($min_id_run)";
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

if (defined $max_id_run and defined $min_id_run) {
  push @id_run, $min_id_run .. $max_id_run;
}

@{$collections} or
  pod2usage(-msg     => 'At least one --collection argument is required',
            -exitval => 2);

@id_run = uniq sort @id_run;

my $irods = $dry_run      ?
  WTSI::NPG::DriRODS->new :
  WTSI::NPG::iRODS->new;

my $logger = Log::Log4perl->get_logger('main');

my @composition_paths = _read_composition_paths_stdin($logger, $stdio);

push @composition_paths,
  _filter_composition_paths_irods($logger, $zone, $collections, \@id_run);
@composition_paths = uniq sort @composition_paths;

$logger->info('Processing ', scalar @composition_paths, ' composition paths');

my $num_updated = 0;

if (@composition_paths) {
  my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

  my @init_args = (mlwh_schema => $wh_schema);
  if ($driver_type) {
    $logger->info("Overriding default driver type with '$driver_type'");
    push @init_args, 'driver_type' => $driver_type;
  }
  my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(@init_args);

  $num_updated = WTSI::NPG::HTS::Illumina::MetaUpdater->new
    (irods        => $irods,
     lims_factory => $lims_factory)->update_secondary_metadata
       (\@composition_paths);
}

$logger->info("Updated metadata on $num_updated files");

exit 0;

__END__

=head1 NAME

npg_update_illumina_metadata

=head1 SYNOPSIS

npg_update_illumina_metadata [--dry-run] [--logconf file]
  --min-id-run id_run --max-id-run id_run | --id-run id_run
  [--collection] [--verbose] [--zone name]

 Options:

  --debug       Enable debug level logging. Optional, defaults to false.
  --dry-run
  --dry_run     Enable dry-run mode. Propose metadata changes, but do not
                perform them. Optional, defaults to true.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --max-id-run
  --max_id_run  The upper limit of a run number range to update. Optional.
  --min-id-run
  --min_id_run  The lower limit of a run number range to update. Optional.
  --id-run
  --id_run      A specific run to update. May be given multiple times to
                specify multiple runs. If used in conjunction with --min-run
                and --max-run, the union of the two sets of runs will be
                updated.
  --collection  A list of iRODS paths where the run collection might be found.
                At least one collection is required.
  --verbose     Print messages while processing. Optional.
  --zone        Zone name. Optional, defaults to 'seq'.
  -             Read iRODS paths from STDIN instead of finding them by their
                run, lane and tag index.

 Advanced options:

  --driver-type
  --driver_type Set the ML warehouse driver type to a custom value.

=head1 DESCRIPTION

This script updates secondary metadata (i.e. LIMS-derived metadata,
not primary experimental metadata) on sequencing result files in
iRODS. The files may be specified by run, in which case either a
specific run or run range must be given, alternatively a list of iRODS
paths may be piped to STDIN.

This script will update metadata on all the files that constitute a
run, including ancillary files, JSON files and genotype files. Some of
these files do not have sufficient metadata to identify them as
belonging to a specific run, using metadata alone. This script uses
an Illumina::ResultSet to determine which files to update.

In dry run mode, the proposed metadata changes will be written as INFO
notices to the log.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018, 2019, 2023 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
