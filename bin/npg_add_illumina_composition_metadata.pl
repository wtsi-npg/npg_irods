#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[uniq];
use Log::Log4perl::Level;
use Pod::Usage;
use English qw[-no_match_vars];
use Try::Tiny;
use File::Basename;
use Readonly;
use Carp;

use WTSI::NPG::DriRODS;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw[ $ALIGNMENT_FILTER 
                                   $COMPOSITION
                                   $COMPONENT
                                   $ID_PRODUCT
                                   $ID_RUN
                                   $POSITION
                                   $TAG_INDEX ];
use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_pipeline::product;

our $VERSION = '0';

Readonly::Scalar my $DEFAULT_ZONE => 'seq';
Readonly::Scalar my $THOUSAND     => 1000;

sub _read_run_ids_stdin {
  my ($log, $stdio) = @_;

  my @id_run;
  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @id_run, $line;
    }

    $log->info('Read ' . scalar @id_run . ' run ids from STDIN');
  }

  return @id_run;
}

sub _filter_file_paths_irods {
  my ($logger, $zone, $collections, $id_run) = @_;

  my @filtered;

  foreach my $collection (@{$collections}) {
    $logger->debug('Working on collection ', $collection);
    my @paths = _find_file_paths_irods($logger, $zone, $collection);

    my @filter;
    foreach my $id (@{$id_run}) {
      my $d = int($id / $THOUSAND);
      push @filter, "\Q$collection/$d/$id/\E", "\Q$collection/$id/\E";
    }

    my $pattern = sprintf q[^(%s)], join q[|], @filter;
    $logger->debug('Filtering with ', $pattern);
    my $re = qr{$pattern}msx;
    push @filtered, grep { not m{cellranger|longranger}msx }
                    grep { m{$re}msx } @paths;
  }

  $logger->debug('Found paths: ', pp(\@filtered));

  return @filtered;
}

sub _find_file_paths_irods {
  my ($logger, $zone, $collection) = @_;

  my @paths = ();

  for my $ext (qw/cram bam/) {
    my $query =
     sprintf q[select COLL_NAME, DATA_NAME where ] .
             q[COLL_NAME like '%s/%%' ]          .
             q[and DATA_NAME like '%%.%s'], $collection, $ext;

    $logger->debug('Running ', $query);
    my $iquest = WTSI::DNAP::Utilities::Runnable->new
      (executable => 'iquest',
       arguments  => ['-z', $zone, '--no-page', q[%s/%s], $query])->run;

    push @paths, ($iquest->split_stdout);
  }

  return @paths;
}

sub _create_irods_composition_file {
  my ($cpath, $composition_json) = @_;

  #####
  # -f to overwrite existing file
  # We are unlikely to run this script twice on the same run,
  # but we might have both bam and cram file for the same
  # entity 
  open my $fh, q[|-], "tears -w $cpath -f" or
    croak "Fail to open a pipe to tears $OS_ERROR";
  print $fh $composition_json or
    croak "Fail to write to a pipe to tears $OS_ERROR";
  close $fh or carp "Failed to close the pipe $OS_ERROR";
  $CHILD_ERROR and croak "Error in tears: $CHILD_ERROR";

  return;
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
my $dry_run = 1;
my $report_path;
my @id_run;
my $log4perl_config;
my $max_id_run;
my $min_id_run;
my $stdio;
my $verbose;
my $zone = $DEFAULT_ZONE;

GetOptions('collection=s@'             => \$collections,
           'debug'                     => \$debug,
           'dry-run|dry_run!'          => \$dry_run,
           'report-path|report_path=s' => \$report_path,
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
} else {
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

my $irods = $dry_run      ?
  WTSI::NPG::DriRODS->new :
  WTSI::NPG::iRODS->new;

my $logger = Log::Log4perl->get_logger('main');

if (!@id_run) {
  @id_run = _read_run_ids_stdin($logger, $stdio);
}
@id_run = uniq sort {$a <=> $b} @id_run;
@id_run or pod2usage(-msg     => 'At least one run id is required',
                     -exitval => 2);

my @file_paths =
  _filter_file_paths_irods($logger, $zone, $collections, \@id_run);
@file_paths = uniq sort @file_paths;

my $num_files2process = scalar @file_paths;
$logger->info("Processing $num_files2process file paths");

my $num_updated = 0;
my $num_skipped = 0;
my $num_mismatches = 0;
my $num_created = 0;
my $report_fh;

if ($report_path) {
  open $report_fh, q[>], $report_path or $logger->logcroak(
    "Failed to open $report_path for writing: $OS_ERROR");
}

for my $path (@file_paths) {

  # Attributes we are insterested in should be defined once only
  my %meta = map { $_->{'attribute'} => $_->{'value'} }
             $irods->get_object_meta($path);
  if ($meta{$ID_PRODUCT}) {
    $num_skipped++;
    $logger->debug("Skipping $path, $ID_PRODUCT=", $meta{$ID_PRODUCT});
    next;
  }

  my $composition_json = $meta{$COMPOSITION};
  my $composition;
  if ($composition_json) {
    $composition = npg_tracking::glossary::composition->thaw(
      $composition_json, component_class =>
      'npg_tracking::glossary::composition::component::illumina');
  } else {
    my $method = $path =~ m{/illumina/runs}smx ? 'logcroak' : 'debug';
    $logger->$method('No composition metadata for ', $path);

    my %attrs = ();
    for my $attr ($ID_RUN, $POSITION, $TAG_INDEX, $ALIGNMENT_FILTER) {
      if (defined $meta{$attr}) {
        if ($attr eq $ALIGNMENT_FILTER) {
          $attrs{'subset'} = $meta{$attr};
        } elsif ($attr eq $POSITION) {
          $attrs{'position'} = $meta{$attr};
	} else {
          $attrs{$attr} = $meta{$attr};
        }
      }
    }

    my ($ifile_name, $idir) = fileparse $path;

    my $file_name_mismatch = 0;
    my $subset = q[];
    my $subset_mismatch = 0;

    ($attrs{'id_run'} && $attrs{'position'}) or $logger->logcroak(
      "Either id_run or position is not defined for $path");

    $composition = npg_tracking::glossary::composition->new(
      components => [
        npg_tracking::glossary::composition::component::illumina->new(\%attrs)
      ]
    );
    my $product = npg_pipeline::product->new(composition => $composition);
    my $expected_name = $product->file_name_root(); #? with subset
    if ($ifile_name !~ /\A$expected_name\.(?bam|cram)\Z/xms) {
      $logger->warn('File name mismatch: ', $path, ' and ', $expected_name);
      $file_name_mismatch = 1;
    }

    if ($attrs{'subset'}) {
      $subset = $attrs{'subset'};
      if ($ifile_name !~ /$subset/smx) {
        $logger->error(
          "Subset $subset mismatch with file name in $path, skipping");
        $subset_mismatch = 1;
      }
    }

    if ($report_path and ($file_name_mismatch or $subset_mismatch)) {
      my $line = join q[,], $path, $file_name_mismatch,
                            $subset, $subset_mismatch;
      print $report_fh $line or $logger->logcroak(
          'Failed to write to ' , $report_path, q[: ], $OS_ERROR);
    }

    $composition_json = $composition->freeze();
    $logger->info("Generated composition for ${path}: $composition_json");

    if ($subset_mismatch) {
      $num_mismatches++;
      next;
    }

    # Create a composition file.
    my $cpath = $idir . $expected_name . q[.composition.json];
    try {
      $num_created++;
      $dry_run or
      _create_irods_composition_file($cpath, $composition_json);
    } catch {
      $logger->logcroak("Failed to create file ${cpath}: $_");
    };
    # Will not deal with ACLs for the new file here, the metadata updater
    # can fix them later if necessary. The same for secondary metadata.
    # Setting the same metadata on a file gives an error.
    try {
      $irods->add_object_avu($cpath, 'type', 'json');
    } catch {
      $logger->error("Error setting type metadata on ${cpath}: $_");
    };

    # Add composition and component metadata where they we
    # previously missing.
    try {
      $irods->add_object_avu($path, $COMPOSITION, $composition_json);
    } catch {
      $logger->error("Error setting $COMPOSITION metadata on ${path}: $_");
    };
    try {
      $irods->add_object_avu($path, $COMPONENT,
                             $composition->get_component(0)->freeze);
    } catch {
      $logger->error("Error setting $COMPONENT metadata on ${path}: $_");
    };
  }

  # Add id_product (composition digest) metadata
  $irods->add_object_avu($path, $ID_PRODUCT, $composition->digest);
  $num_updated++;
}

if ($report_fh) {
  close $report_fh or $logger->warn(
    'Failed to close a file handle to ', $report_path, q[: ], $OS_ERROR);
}

$logger->info("Skipps summary: $num_skipped");
$logger->info("Mismatches summary: $num_mismatches");
$logger->info("Updates summary: $num_updated");
$logger->info("Creates summary: $num_created");

exit 0;

__END__

=head1 NAME

npg_add_illumina_composition_metadata

=head1 SYNOPSIS

npg_add_illumina_composition_metadata [--dry-run] [--logconf file]
  --min-id-run id_run --max-id-run id_run | --id-run id_run
  [--collection] [--verbose] [--zone name]

 Options:

  --debug       Enable debug level logging. Optional, defaults to false.
  --dry-run
  --dry_run     Enable dry-run mode. Propose metadata changes, but do not
                perform them. Optional, defaults to true.
  --help        Display help.
  --report-path
  --report_path Full file path for a structured report. Optional.
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

=head1 DESCRIPTION

This script borrows heavily from npg_update_illumina_metadata.pl
It adds composition-related metadata to and creates composition
files for Illumina sequencing data (cram and bam files) that already
exist in iRODS.

id_run position, tag_index and alignment_filter metadata are
used to infer the composition.

If the report_path or report-path argument is set, a CSV report
listing all mismatches is generated.

In dry run mode, the proposed changes will be written as INFO
notices to the log.

=head1 AUTHOR

Marina Gourtovaia <mg8@sanger.ac.uk>

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
