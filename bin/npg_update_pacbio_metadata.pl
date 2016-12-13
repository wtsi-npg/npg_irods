#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[uniq];
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::DriRODS;
use WTSI::NPG::HTS::PacBio::MetaUpdater;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw[$FILE_TYPE];

our $VERSION = '';
our $DEFAULT_ZONE = 'seq';

my $debug;
my $dry_run = 1;
my @id_run;
my $log4perl_config;
my $max_id_run;
my $min_id_run;
my $stdio;
my $verbose;
my $zone;

GetOptions('debug'                     => \$debug,
           'dry-run|dry_run!'          => \$dry_run,
           'help'                      => sub { pod2usage(-verbose => 2,
                                                          -exitval => 0) },
           'logconf=s'                 => \$log4perl_config,
           'verbose'                   => \$verbose,
           'max_id_run|max-id-run=i'   => \$max_id_run,
           'min_id_run|min-id-run=i'   => \$min_id_run,
           'id_run|id-run=i'           => \@id_run,
           'zone=s',                   => \$zone,
           q[]                         => \$stdio);

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
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
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

@id_run = uniq sort @id_run;

$zone ||= $DEFAULT_ZONE;

# Setup iRODS
my $irods;
if ($dry_run) {
  $irods = WTSI::NPG::DriRODS->new;
}
else {
  $irods = WTSI::NPG::iRODS->new;
}

# Find data objects
my @data_objs;
if ($stdio) {
  binmode \*STDIN, 'encoding(UTF-8)';

  $log->info('Reading iRODS paths from STDIN');
  while (my $line = <>) {
    chomp $line;
    push @data_objs, $line;
  }
}

# Range queries in iRODS are so slow that we have to do lots of
# per-run queries
foreach my $id_run (@id_run) {
  # Find the annotated objects by query
  my @query = _make_run_query($id_run);
  $log->info('iRODS query: ', pp(\@query));
  push @data_objs, $irods->find_objects_by_meta("/$zone", @query);
}

@data_objs = uniq sort @data_objs;
$log->info('Processing ', scalar @data_objs, ' data objects');

# Update metadata
my $num_updated = 0;

if (@data_objs) {
  my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

  $num_updated = WTSI::NPG::HTS::PacBio::MetaUpdater->new
    (irods       => $irods,
     mlwh_schema => $wh_schema)->update_secondary_metadata(\@data_objs);
}

$log->info("Updated metadata on $num_updated files");

sub _make_run_query {
  my ($q_id_run) = @_;

  my @query = ([$FILE_TYPE => 'h5']);
  if (defined $q_id_run) {
    push @query, ['run' => $q_id_run];
  }

  return @query;
}

__END__

=head1 NAME

npg_update_pacbio_metadata

=head1 SYNOPSIS

npg_update_pacbio_metadata [--dry-run] [--logconf file]
  --min-id-run id_run --max-id-run id_run | --id-run id_run
  [--verbose] [--zone name]

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
  --verbose     Print messages while processing. Optional.
  --zone        The iRODS zone in which to work. Optional, defaults to 'seq'.
  -             Read iRODS paths from STDIN instead of finding them by their
                run, lane and tag index.

=head1 DESCRIPTION

This script updates secondary metadata (i.e. LIMS-derived metadata,
not primary experimental metadata) on PacBio data files in iRODS. The
files may be specified by run in which case either a specific run or
run range must be given. Additionally a list of iRODS paths may be piped
to STDIN.

This script will update metadata on all the H5 files that constitute a
run.

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
