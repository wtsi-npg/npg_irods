#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[none];
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::DriRODS;
use WTSI::NPG::HTS::Illumina::RunPublisher;
use WTSI::NPG::HTS::LIMSFactory;

our $VERSION = '';
our $DEFAULT_ZONE = 'seq';

my $embedded_conf = << 'LOGCONF';
   log4perl.logger = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
LOGCONF

my $alignment;
my $alt_process;
my $ancillary;
my $archive_path;
my $collection;
my $debug;
my $driver_type;
my $file_format;
my $index;
my $log4perl_config;
my $qc;
my $runfolder_path;
my $verbose;

my @positions;

GetOptions('alignment'                         => \$alignment,
           'alt-process|alt_process=s'         => \$alt_process,
           'ancillary'                         => \$ancillary,
           'archive-path|archive_path=s'       => \$archive_path,
           'collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'driver-type|driver_type=s'         => \$driver_type,
           'file-format|file_format=s'         => \$file_format,
           'help'                              => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'index'                             => \$index,
           'logconf=s'                         => \$log4perl_config,
           'lanes|positions=i'                 => \@positions,
           'qc'                                => \$qc,
           'runfolder-path|runfolder_path=s'   => \$runfolder_path,
           'verbose'                           => \$verbose);

# Process CLI arguments
if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  Log::Log4perl::init(\$embedded_conf);
}

my $log = Log::Log4perl->get_logger(q[]);
if ($verbose and not $debug) {
  $log->level($INFO);
}
elsif ($debug) {
  $log->level($DEBUG);
}

if (not $file_format) {
  $file_format = 'cram';
}
$file_format = lc $file_format;

if (not (defined $runfolder_path or defined $archive_path)) {
  my $msg = 'A --runfolder-path or --archive-path argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

# Setup iRODS
my $irods = WTSI::NPG::iRODS->new(logger => $log);

my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

# Make this an optional argument (construct within the publisher)
my @fac_init_args = (mlwh_schema => $wh_schema);
if ($driver_type) {
  $log->info("Overriding default driver type with '$driver_type'");
  push @fac_init_args, 'driver_type' => $driver_type;
}
my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(@fac_init_args);

my @pub_init_args = (file_format     => $file_format,
                     irods           => $irods,
                     lims_factory    => $lims_factory,
                     logger          => $log);

if (defined $archive_path) {
  push @pub_init_args, archive_path => $archive_path;
}
if (defined $runfolder_path) {
  push @pub_init_args, runfolder_path => $runfolder_path;
}
if ($collection) {
  push @pub_init_args, dest_collection => $collection;
}
if ($alt_process) {
  push @pub_init_args, alt_process => $alt_process;
  $log->info("Using alt_process '$alt_process'");
}

my $publisher = WTSI::NPG::HTS::Illumina::RunPublisher->new(@pub_init_args);

my ($num_files, $num_published, $num_errors) = (0, 0, 0);
my $increment_counts = sub {
  my ($nf, $np, $ne) = @_;

  $num_files     += $nf;
  $num_published += $np;
  $num_errors    += $ne;
};

# Default to all available positions
if (not @positions) {
  @positions = $publisher->positions;
}

if (none { defined } ($alignment, $ancillary, $index, $qc)) {
  $increment_counts->($publisher->publish_files
                      (positions => \@positions));
}
else {
  if ($alignment) {
    $increment_counts->($publisher->publish_alignment_files
                        (positions => \@positions));
  }
  if ($index) {
    $increment_counts->($publisher->publish_index_files
                        (positions => \@positions));
  }
  if ($ancillary) {
    $increment_counts->($publisher->publish_ancillary_files
                        (positions => \@positions));
  }
  if ($qc) {
    $increment_counts->($publisher->publish_qc_files
                        (positions => \@positions));
  }
}

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

npg_publish_illumina_run

=head1 SYNOPSIS

npg_publish_illumina_run --runfolder-path <path> [--collection <path>]
  [--file-format <format>] [--position <n>]*
  [--alignment] [--index] [--ancillary] [--qc]
  [--debug] [--verbose] [--logconf <path>]

 Options:
   --alignment       Load alignment files. Optional, defaults to true.
   --alt-process
   --alt_process     Alternative process used. Optional.
   --ancillary       Load ancillary (any file other than alignment, index
                     or JSON). Optional, defaults to true.
   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/<id_run>/.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --file-format
   --file_format     Load alignment files of this format. Optional,
                     defaults to CRAM format.
   --help            Display help.
   --index           Load alignment index files. Optional, defaults to
                     true.
   --lanes
   --positions       A sequencing lane/position to load. This option may
                     be supplied multiple times to load multiple lanes.
                     Optional, defaults to loading all available lanes.
   --qc              Load QC JSON files. Optional, defaults to true.
   --runfolder-path
   --runfolder_path  The instrument runfolder path to load.
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

 Advanced options:

  --driver-type
  --driver_type Set the ML warehouse driver type to a custom value.

=head1 DESCRIPTION

This script loads data and metadata for a single Illumina sequencing
run into iRODS.

Data files are divided into four categories:

 - alignment files; the sequencing reads in BAM or CRAM format.
 - alignment index files; indices in the relevant format
 - ancillary files; files containing information about the run
 - QC JSON files; JSON files containing information about the run

As part of the copying process, metadata are added to, or updated on,
the files in iRODS. Following the metadata update, access permissions
are set. The information to do both of these operations is provided by
an instance of st::api::lims.

If a run is published multiple times to the same destination
collection, the following take place:

 - the script checks local (run folder) file checksums against remote
   (iRODS) checksums and will not make unnecessary updates

 - if a local file has changed, the copy in iRODS will be overwritten
   and additional metadata indicating the time of the update will be
   added

 - the script will proceed to make metadata and permissions changes to
   synchronise with the metadata supplied by st::api::lims, even if no
   files have been modified

The default behaviour of the script is to publish all four categories
of file (alignment, index, ancillary, qc), for all available lane
positions. This may be restricted by using one or more of the command
line flags --alignment, --index, --ancillary and --qc, each of which
instructs the script to act on only that type of
file. e.g. "--alignment --index" will cause just alignment and index
files to be copied. Specifying all four of these flags at once is
equivalent to the default behaviour (i.e. all files).

One or more "--position <position>" arguments may be supplied to
restrict operations specific lanes. e.g. "--position 1 --position 8"
will publish from lane positions 1 and 8 only.

If an alternative process has been used, it may be supplied as a
string using the "--alt-process <name>" argument. This affects the
metadata in iRODS (resulting in "target = 0", "alt_target = 1",
"alt_process = <name>"). It also affects the default destination
collection in iRODS, which will have an extra leaf collection added,
having the name of the "--alt-process <name>" argument. If the
destination collection is set explicitly on the command line, the
extra leaf collection isn not added.

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
