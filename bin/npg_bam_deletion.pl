#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::DriRODS;
use WTSI::NPG::Data::BamDeletion;
use WTSI::NPG::iRODS;

our $VERSION = '';

my $debug;
my $log4perl_config;
my $irods_file;
my $irods_fofn;
my $verbose;
my $rt_ticket;
my $dry_run = 1;
my @init_args = ();
my @files = ();
my $outdir;

GetOptions(
           'debug'                             => \$debug,
           'help'                              => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'irods-file|irods_file=s'                   => \$irods_file,
           'irods-fofn|irods_fofn=s'                   => \$irods_fofn,
           'rt-ticket|rt_ticket=i'                    => \$rt_ticket,
           'outdir=s'                       => \$outdir,
           'dry-run|dry_run!'               => \$dry_run,
           'logconf=s'                      => \$log4perl_config,
           'verbose'                        => \$verbose);

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

if (not (defined $irods_file or defined $irods_fofn) ) {
  pod2usage(-msg     => 'irods_file or irods_fofn argument is required',
            -exitval => 2);
}
if (not defined $rt_ticket){
  pod2usage(-msg     => 'rt_ticket argument is required',
            -exitval => 2);
}

if ($dry_run){
  push @init_args, irods          => WTSI::NPG::DriRODS->new;
}
else{
  push @init_args, irods          => WTSI::NPG::iRODS->new;
}
  push @init_args, rt_ticket      => $rt_ticket;
  push @init_args, dry_run        => $dry_run;

if ($outdir){
  push @init_args, outdir  => $outdir;
}

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

if (defined $irods_file) {
  push @files,$irods_file;
}

if (defined $irods_fofn){
  my $fh;
   open $fh, '<', $irods_fofn || $log->logcroak(q[Failed to open file '], $irods_fofn, q[']);
   while(my $file = <$fh>){
     chomp $file;
     if ($file =~/[.]\S+$/mxs){
     $log->debug('Found ' . $file);
     push @files,$file;
    }
  }
}

foreach my $file (@files){
 $log->info("Processing $file");
 push  @init_args, file => $file;
 my $path =
  WTSI::NPG::Data::BamDeletion->new(@init_args)->process;

   if ($dry_run){
       $log->info("Would be removing data from '$path'");
   }
   else {
       $log->info("Removed data from '$path'");
   }
}

__END__

=head1 NAME

npg_bam_deletion.pl

=head1 SYNOPSIS

npg_bam_deletion --irods_file <path> [--irods_fofn <file of iRODS file paths>] --rt_ticket <int>
   [--debug] [--verbose] [--logconf <path>] [--nodry_run] [--outdir <path>]

 Options:
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --help            Display help.
   --irods_file      Path to iRODS file
   --irods_fofn      File of paths to iRODS files
   --rt_ticket       RT ticket number for this request
   --outdir          Where to write header/comment file prior to loading to iRODS [defaults to /tmp sub-directory]
   --nodry_run       Default is dry_run
   --logconf         A log4perl configuration file. Optional.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

This script replaces the specified consent withdrawn file(s) with a header file or a file with a deletion comment.
It also updates the meta data for target and adds the rt_ticket.  

=head1 AUTHOR

Jillian Durham <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
