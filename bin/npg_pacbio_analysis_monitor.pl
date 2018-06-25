#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;


use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::iRODS;
use WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor;

our $VERSION = '';

my $collection;
my $debug;
my $log4perl_config;
my $pipeline_name;
my $task_name;
my $verbose;

GetOptions('collection=s'                  => \$collection,
           'debug'                         => \$debug,
           'help'                          => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'logconf=s'                     => \$log4perl_config,
           'pipeline-name|pipeline_name=s' => \$pipeline_name,
           'task-name|task_name=s'         => \$task_name,
           'verbose'                       => \$verbose);


if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

my $irods     = WTSI::NPG::iRODS->new;
my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

my @init_args = (irods              => $irods,
                 mlwh_schema        => $wh_schema);
if ($collection) {
  push @init_args, dest_collection => $collection;
}

if ($pipeline_name) {
  push @init_args, pipeline_name => $pipeline_name;
}

if ($task_name) {
  push @init_args, task_name => $task_name;
}

my $monitor = WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor->new
  (@init_args);

my ($num_files, $num_published, $num_errors) =
  $monitor->publish_analysed_cells;

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

npg_pacbio_analysis_monitor

=head1 SYNOPSIS

npg_pacbio_analysis_monitor
  [--collection <path>] [--debug] [--logconf <path>]
  [--pipeline_name <name>] [--task_name <name>]
  [--verbose] 

 Options:
   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/pacbio/.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --pipeline-name
   --pipeline_name   The SMRT Link pipeline name. Optional.  
   --task-name
   --task_name       The SMRT Link task name. Optional.  
   --verbose         Print messages while processing. Optional.



=head1 DESCRIPTION
 
This script queries a PacBio web service for successful analysis jobs 
and loads demultiplexed into iRODS using the relevant PacBio RunPublisher 
module.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
