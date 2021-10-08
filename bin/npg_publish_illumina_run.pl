#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[none];
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::Illumina::RunPublisher;
use WTSI::NPG::HTS::LIMSFactory;

our $VERSION = '';
our $DEFAULT_ZONE = 'seq';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.Illumina = INFO, A1
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

my $alt_process;
my $dest_collection;
my $debug;
my $driver_type;
my $force = 0;
my $id_run;
my $log4perl_config;
my $max_errors = 0;
my $restart_file;
my $source_directory;
my $verbose;

my @include;
my @exclude;

GetOptions('alt-process|alt_process=s'           => \$alt_process,
           'collection=s'                        => \$dest_collection,
           'debug'                               => \$debug,
           'driver-type|driver_type=s'           => \$driver_type,
           'exclude=s'                           => \@exclude,
           'force'                               => \$force,
           'help'                                => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'id_run|id-run=i'                     => \$id_run,
           'include=s'                           => \@include,
           'logconf=s'                           => \$log4perl_config,
           'max-errors|max_errors=i'             => \$max_errors,
           'restart-file|restart_file=s'         => \$restart_file,
           'source-directory|source_directory=s' => \$source_directory,
           'verbose'                             => \$verbose);

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

if (not defined $source_directory) {
  my $msg = 'A --source-directory argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

my $irods = WTSI::NPG::iRODS->new;

my @fac_init_args = ();
if ($driver_type) {
  $log->info("Overriding default driver type with '$driver_type'");
  push @fac_init_args, 'driver_type' => $driver_type;
}

my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(@fac_init_args);

my @pub_init_args = (exclude          => \@exclude,
                     force            => $force,
                     include          => \@include,
                     irods            => $irods,
                     lims_factory     => $lims_factory,
                     source_directory => $source_directory);
if ($id_run) {
  push @pub_init_args, id_run => $id_run;
}
if ($dest_collection) {
  push @pub_init_args, dest_collection => $dest_collection;
}
if ($alt_process) {
  push @pub_init_args, alt_process => $alt_process;
  $log->info("Using alt_process '$alt_process'");
}
if ($restart_file) {
  push @pub_init_args, restart_file => $restart_file;
  $log->info("Using restart_file '$restart_file'");
}
if ($max_errors) {
  push @pub_init_args, max_errors => $max_errors;
}

my $publisher = WTSI::NPG::HTS::Illumina::RunPublisher->new(@pub_init_args);

use sigtrap 'handler', \&handler, 'normal-signals';

sub handler {
  my ($signal) = @_;

  $log->info('Writing restart file ', $publisher->restart_file);
  $publisher->write_restart_file;
  $log->error("Exiting due to $signal");
  exit 1;
}

my ($num_files, $num_published, $num_errors) = $publisher->publish_files;

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

npg_publish_illumina_run --source-directory <path> [--collection <path>]
  [--file-format <format>] [--force] [--max-errors <n>]
  [--debug] [--verbose] [--logconf <path>]

 Options:
   --alt-process
   --alt_process      Alternative process used. Optional.
   --collection       The destination collection in iRODS. Optional,
                      defaults to /seq/<id_run>/.
   --debug            Enable debug level logging. Optional, defaults to
                      false.
   --exclude          Specifiy one or more regexes to ignore paths under
                      the target collection. Matching paths will be not be
                      published. If more than one regex is supplied, they
                      are all applied. Exclude regexes are applied after
                      any include regexes (see below).
   --force            Force an attempt to re-publish files that have been
                      published successfully.
   --help             Display help.
   --id-run
   --id_run           Specify the run number. Optional, defaults to the
                      value detected from the runfolder. This option is
                      useful for runs where the value cannot be detected
                      automatically.
   --include          Specifiy one or more regexes to select paths under
                      the target collection. Only matching paths will be
                      published, all others will be ignored. If more than
                      one regex is supplied, the matches for all of them
                      are aggregated.

   --max-errors       The maximum number of errors permitted before aborting.
                      Optional, defaults to unlimited.
   --restart-file
   --restart_file     A file path where a record of successfully published
                      files will be recorded in JSON format on exit. If the
                      jobs is restarted, no attempt will be made to publish
                      or even check these files in iRODS. Optional. The
                      default restart file is "<archive dir>/published.json".
   --source-directory
   --source_directory The instrument runfolder path to load.
   --logconf          A log4perl configuration file. Optional.
   --verbose          Print messages while processing. Optional.

 Advanced options:

  --driver-type
  --driver_type Set the lims driver type to a custom value. The default
                is driver type is 'ml_warehouse_fc_cache' (defined by
                WTSI::NPG::HTS::LIMSFactory). Other st::spi::lims driver
                types may be used e.g. 'samplesheet'.

=head1 DESCRIPTION

This script loads data and metadata for a single Illumina sequencing
run into iRODS.

Data files are divided into seven categories:

 - alignment files; the sequencing reads in CRAM format.
 - alignment index files; indices in the relevant format.
 - ancillary files; files containing information about the run.
 - genotype files; files containing genotypes from sequencing data.
 - QC JSON files; JSON files containing information about the run.
 - XML files; XML files describing the entire run.
 - InterOp files; binary files containing diagnostic data for the
   entire run.

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

The behaviour of the script is to publish all seven categories of file
(alignment, ancillary, genotype, index, interop, qc and xml), for all
available lane positions.

If an alternative process has been used, it may be supplied as a
string using the "--alt-process <name>" argument. This affects the
metadata in iRODS (resulting in "target = 0", "alt_target = 1",
"alt_process = <name>"). It also affects the default destination
collection in iRODS, which will have an extra leaf collection added,
having the name of the "--alt-process <name>" argument. If the
destination collection is set explicitly on the command line, the
extra leaf collection is not added.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2017, 2018, 2019 Genome Research Limited. All Rights
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
