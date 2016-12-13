#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::DriRODS;
use WTSI::NPG::HTS::Illumina::LogPublisher;
use WTSI::NPG::iRODS;

our $VERSION = '';

my $collection;
my $debug;
my $id_run;
my $log4perl_config;
my $runfolder_path;
my $verbose;

GetOptions('collection=s'                      => \$collection,
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'id_run|id-run=i'                   => \$id_run,
           'logconf=s'                         => \$log4perl_config,
           'runfolder-path|runfolder_path=s'   => \$runfolder_path,
           'verbose'                           => \$verbose);

# Process CLI arguments
if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

if (not defined $runfolder_path) {
  pod2usage(-msg     => 'A runfolder path argument is required',
            -exitval => 2);
}

my @init_args = (irods          => WTSI::NPG::iRODS->new,
                 runfolder_path => $runfolder_path);
if ($collection) {
  push @init_args, dest_collection => $collection;
}
if (defined $id_run) {
  push @init_args, id_run => $id_run;
}

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

my $path =
  WTSI::NPG::HTS::Illumina::LogPublisher->new(@init_args)->publish_logs;

$log->info("Published logs to '$path'");

__END__

=head1 NAME

npg_publish_illumina_logs

=head1 SYNOPSIS

npg_publish_illumina_logs --runfolder-path <path> [--collection <path>]
  [--id-run <id_run>] [--debug] [--verbose] [--logconf <path>]

 Options:
   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/<id_run>/log.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --runfolder-path
   --runfolder_path  The instrument runfolder path to load.
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

This script loads log files and metadata for a single Illumina
sequencing run into iRODS.

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
