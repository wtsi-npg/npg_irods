#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::PacBio::RunPublisher;
use WTSI::NPG::iRODS;

our $VERSION = '';

my $collection;
my $debug;
my $log4perl_config;
my $runfolder_path;
my $verbose;

GetOptions('collection=s'                    => \$collection,
           'debug'                           => \$debug,
           'help'                            => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'logconf=s'                       => \$log4perl_config,
           'runfolder-path|runfolder_path=s' => \$runfolder_path,
           'verbose'                         => \$verbose);

# Process CLI arguments
if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $ERROR;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

if (not (defined $runfolder_path)) {
  my $msg = 'A --runfolder-path argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

my $irods     = WTSI::NPG::iRODS->new;
my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

my @init_args = (irods          => $irods,
                 mlwh_schema    => $wh_schema,
                 runfolder_path => $runfolder_path);
if ($collection) {
  push @init_args, dest_collection => $collection;
}

my $publisher = WTSI::NPG::HTS::PacBio::RunPublisher->new(@init_args);
my ($num_files, $num_published, $num_errors) = $publisher->publish_files;

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

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

npg_publish_pacbio_run

=head1 SYNOPSIS

npg_publish_pacbio_run --runfolder-path <path> [--collection <path>]
  [--debug] [--verbose] [--logconf <path>]

 Options:
   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/pacbio/.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --runfolder-path
   --runfolder_path  The instrument runfolder path to load.
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

This script loads data and metadata for a single PacBio sequencing run
into iRODS.

As part of the copying process, metadata are added to, or updated on,
the files in iRODS. Following the metadata update, access permissions
are set. The information to do both of these operations is provided by
the ML warehouse.

If a run is published multiple times to the same destination
collection, the following take place:

 - the script checks local (run folder) file checksums against remote
   (iRODS) checksums and will not make unnecessary updates

 - if a local file has changed, the copy in iRODS will be overwritten
   and additional metadata indicating the time of the update will be
   added

 - the script will proceed to make metadata and permissions changes to
   synchronise with the metadata supplied by the ML warehouse, even if
   no files have been modified

The default behaviour of the script is to publish all categories of
file (metadata XML, bas/x.h5 and sts XML), for all available SMRT
cells.

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
