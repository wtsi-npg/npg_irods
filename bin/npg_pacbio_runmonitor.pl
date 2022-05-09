#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;
use Readonly;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::iRODS;
use WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;


our $VERSION = '';

Readonly::Scalar my $DEFAULT_INTERVAL_DAYS   => 14;
Readonly::Scalar my $DEFAULT_OLDER_THAN_DAYS => 0;
Readonly::Scalar my $MODE_GROUP_WRITABLE     => q(0020);

my $api_uri;
my $collection;
my $debug;
my $execute = 1;
my $interval = $DEFAULT_INTERVAL_DAYS;
my $local_path;
my $log_dir;
my $log4perl_config;
my $older_than = $DEFAULT_OLDER_THAN_DAYS;
my $submit_wr;
my $verbose;

GetOptions('api-uri|api_uri=s'       => \$api_uri,
           'collection=s'            => \$collection,
           'debug'                   => \$debug,
           'help'                    => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'execute!'                => \$execute,
           'interval=i'              => \$interval,
           'logconf=s'               => \$log4perl_config,
           'local-path|local_path=s' => \$local_path,
           'log-dir|log_dir=s'       => \$log_dir,
           'older-than|older_than=i' => \$older_than,
           'submit-wr|submit_wr'     => \$submit_wr,
           'verbose'                 => \$verbose);


my $module = 'WTSI::NPG::HTS::PacBio::Sequel::RunMonitor';

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

if (not $local_path) {
  pod2usage(-msg     => 'A local-path argument is required',
            -exitval => 2);
}

if (!$log_dir || !-d $log_dir) {
  pod2usage(-msg     => 'A log dir must be specified by the log-dir argument and must exist',
            -exitval => 2);
}

my $mode = (stat $log_dir)[2];
if ( not ($mode & $MODE_GROUP_WRITABLE) ) {
  pod2usage(-msg     => 'A log dir specified by the log-dir argument must be group writable',
            -exitval => 2);
}

my $irods     = WTSI::NPG::iRODS->new;
my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

my @init_args = (execute            => $execute,
                 interval           => $interval,
                 irods              => $irods,
                 local_staging_area => $local_path,
                 log_dir            => $log_dir,
                 mlwh_schema        => $wh_schema,
                 older_than         => $older_than,
                 );

if($api_uri) {
  push @init_args, api_uri => $api_uri;
}
if ($collection) {
  push @init_args, dest_collection => $collection;
}
if($submit_wr) {
  push @init_args, submit_wr => $submit_wr;
}

my $monitor = $module->new(@init_args);

my ($num_files, $num_published, $num_errors) =
  $monitor->publish_completed_runs;

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

npg_pacbio_runmonitor

=head1 SYNOPSIS

npg_pacbio_runmonitor --local-path </path/to/staging/area
  [--collection <path>] [--debug] [--execute] [--interval days] 
  [--logconf <path>] [--log-dir <path>] [--execute] [--older-than days] 
  [--submit-wr] [--verbose] [--api-uri]

 Options:
   --collection      The destination collection in iRODS. Optional,
                     defaults to /seq/pacbio/.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --[no]execute     Propose jobs but don't submit them. Optional, defaults
                     to true. Only relevant in conjunction with submit-wr 
                     option.
   --help            Display help.
   --interval        Interval of time in days for run loading. 
                     Optional, defaults to 14.
   --local-path
   --local_path      The path to the local filesystem where result data
                     are staged for loading into iRODS.
   --logconf         A log4perl configuration file. Optional.

   --log-dir
   --log_dir         Path to a log directory which is used in conjunction 
                     with submit-wr option for log files and for run lock 
                     files without the submit-wr option. Directory must exist
                     and be writable.
   --older-than
   --older_than      Only consider runs older than a specified number of 
                     days. Optional defaults to 0 days. 

   --execute         A flag turning on and off execution. True by default.
   --submit-wr
   --submit_wr       Submit individual run loading jobs to a wr manager.
                     Optional, defaults to false.
   --verbose         Print messages while processing. Optional.

   --api-uri   
   --api_uri         Specify the server host and port. Optional.


=head1 DESCRIPTION

This script queries a PacBio web service for runs and loads completed
runs into iRODS using the PacBio RunPublisher module.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2019, 2022 Genome Research Limited. All Rights
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
