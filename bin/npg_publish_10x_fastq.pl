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
use WTSI::NPG::HTS::10x::RunPublisher;
use WTSI::NPG::HTS::LIMSFactory;

our $VERSION = '';
our $DEFAULT_ZONE = 'seq';

my $verbose_config = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.10x = INFO, A1

# Errors from WTSI::NPG::iRODS are propagated in the code to callers
# in WTSI::NPG::HTS::10x, so we do not need to see them directly:

log4perl.logger.WTSI.NPG.iRODS = OFF, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

# The default values cause all file types to be published
my $alt_process;
my $collection;
my $tenx_fastq_path;
my $debug;
my $driver_type;
my $id_run;
my $log4perl_config;
my $runfolder_path;
my $verbose;

my @positions;

GetOptions('alt-process|alt_process=s'         => \$alt_process,
           'tenx_fastq_path=s'                 => \$tenx_fastq_path,
           'collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'driver-type|driver_type=s'         => \$driver_type,
           'help'                              => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'id_run|id-run=i'                   => \$id_run,
           'logconf=s'                         => \$log4perl_config,
           'lanes|positions=i'                 => \@positions,
           'runfolder-path|runfolder_path=s'   => \$runfolder_path,
           'verbose'                           => \$verbose);

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

if (not defined $runfolder_path) {
  my $msg = 'A --runfolder-path is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

# Setup iRODS
my $irods = WTSI::NPG::iRODS->new;

my @fac_init_args = ();
if ($driver_type) {
  $log->info("Overriding default driver type with '$driver_type'");
  push @fac_init_args, 'driver_type' => $driver_type;
}

my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(@fac_init_args);

my @pub_init_args = (irods           => $irods,
                     lims_factory    => $lims_factory,
                     runfolder_path  => $runfolder_path);

if (defined $tenx_fastq_path) {
  push @pub_init_args, tenx_fastq_path => $tenx_fastq_path;
}
if (defined $runfolder_path) {
  push @pub_init_args, runfolder_path=> $runfolder_path;
}
if ($id_run) {
  push @pub_init_args, id_run => $id_run;
}
if ($collection) {
  push @pub_init_args, dest_collection => $collection;
}
if ($alt_process) {
  push @pub_init_args, alt_process => $alt_process;
  $log->info("Using alt_process '$alt_process'");
}

my $publisher = WTSI::NPG::HTS::10x::RunPublisher->new(@pub_init_args);

my ($num_files, $num_published, $num_errors) = (0, 0, 0);
my $inc_counts = sub {
  my ($nf, $np, $ne) = @_;

  $num_files     += $nf;
  $num_published += $np;
  $num_errors    += $ne;
};

# Default to all available positions
if (not @positions) {
  @positions = $publisher->positions;
}

$inc_counts->($publisher->publish_fastq_files(positions => \@positions));

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

npg_publish_10x_fastq

=head1 SYNOPSIS

npg_publish_10x_fastq --runfolder-path <path> [--collection <path>]
  [--tenx_fastq_path <path> [--position <n>]* 
  [--debug] [--verbose] [--logconf <path>]

 Options:
   --alt_process      Alternative process used. Optional.
   --collection       The destination collection in iRODS. Optional,
                      defaults to /seq/<id_run>/.
   --debug            Enable debug level logging. Optional, defaults to
                      false.
   --help             Display help.
   --id-run
   --id_run           Specify the run number. Optional, defaults to the
                      value detected from the runfolder. This option is
                      useful for runs where the value cannot be detected
                      automatically.
   --lanes
   --positions        A sequencing lane/position to load. This option may
                      be supplied multiple times to load multiple lanes.
                      Optional, defaults to loading all available lanes.
   --tenx-fastq-path  
   --tenx_fastq_path  The directory containing the 10x output, it should
                      contain a sub-directory which matches the flowcell
                      as detected from the runfolder.
   --logconf          A log4perl configuration file. Optional.
   --verbose          Print messages while processing. Optional.

 Advanced options:

  --driver-type
  --driver_type Set the lims driver type to a custom value. The default
                is driver type is 'ml_warehouse_fc_cache' (defined by
                WTSI::NPG::HTS::LIMSFactory). Other st::spi::lims driver
                types may be used e.g. 'samplesheet'.

=head1 DESCRIPTION

This script loads fastq and metadata for a single 10X sequencing
run into iRODS.

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

One or more "--position <position>" arguments may be supplied to
restrict operations specific lanes. e.g. "--position 1 --position 8"
will publish from lane positions 1 and 8 only.

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
