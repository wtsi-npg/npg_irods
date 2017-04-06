#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use English qw[-no_match_vars];
use File::Spec::Functions qw[rel2abs];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::MinIONRunPublisher;

our $VERSION = '';

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $collection;
my $debug;
my $log4perl_config;
my $expected_minion_id;
my $runfolder_path;
my $tar_capacity = 10_000;
my $tar_timeout  = 60 * 5;
my $verbose;
##use critic

GetOptions('collection=s'                    => \$collection,
           'debug'                           => \$debug,
           'help'                            => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                       => \$log4perl_config,
           'minion-id=s'                     => \$expected_minion_id,
           'runfolder_path|runfolder-path=s' => \$runfolder_path,
           'tar_capacity|tar-capacity=i'     => \$tar_capacity,
           'tar_timeout|tar-timeout=i'       => \$tar_timeout,
           'verbose'                         => \$verbose);

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
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

$collection or
  $log->logcroak('A collection argument is required');

$expected_minion_id or
  $log->logcroak('A minion-id argument is required');

$runfolder_path or
  $log->logcroak('A runfolder-path argument is required');
-d $runfolder_path or
  $log->logcroak('Invalid runfolder-path argument: not a directory');

$runfolder_path = rel2abs($runfolder_path);

my $publisher = WTSI::NPG::HTS::ONT::MinIONRunPublisher->new
  (dest_collection => $collection,
   minion_id       => $expected_minion_id,
   runfolder_path  => $runfolder_path,
   session_timeout => 200,
   tar_capacity    => $tar_capacity,
   tar_timeout     => $tar_timeout)->publish_files;

__END__

=head1 NAME

npg_publish_minion_run

=head1 SYNOPSIS

npg_publish_minion_run --collection <path> [--debug] [--logconf <path>]
  --minion-id <id> --runfolder-path <path> [--tar-capacity <n>]
  [--tar-timeout <n>] [--verbose]

 Options:
   --collection      The destination collection in iRODS.
   --debug           Enable debug level logging. Optional, defaults to
                     false.
   --file-format
   --file_format     Load alignment files of this format. Optional,
                     defaults to CRAM format.
   --help            Display help.
   --logconf         A log4perl configuration file. Optional.
   --minion-id
   --minion_id       The ID of the MinION whose data are to be published.
   --runfolder-path
   --runfolder_path  The instrument runfolder path to watch.
   --tar-capacity
   --tar_capacity    The number of read files to be archived per tar file.
                     Optional, defaults to 10,000.
   --tar-timeout
   --tar_timeout     The number of seconds idle time after which a tar file
                     open for writing, will be closed. even if it has not
                     reached capacity. Optional, defaults to 300 seconds.
   --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
