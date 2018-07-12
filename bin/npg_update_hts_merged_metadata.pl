#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Data::Dump qw[pp];
use Getopt::Long;
use List::AllUtils qw[uniq];
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::DriRODS;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::Illumina::Merged::MetaUpdater;
use WTSI::NPG::iRODS::Metadata;

our $VERSION        = '';
our $DEFAULT_ZONE   = 'seq';
our $DEFAULT_TARGET = 'library';
our $COMPOSITION    = 'composition'; ## temporary

my $debug;
my $dry_run = 1;
my $log4perl_config;
my $stdio;
my $target;
my $verbose;
my $zone;


GetOptions('debug'                     => \$debug,
           'dry-run|dry_run!'          => \$dry_run,
           'help'                      => sub { pod2usage(-verbose => 2,
                                                          -exitval => 0) },
           'logconf=s'                 => \$log4perl_config,
           'verbose'                   => \$verbose,
           'zone=s',                   => \$zone,
           'target-type|target_type=s' => \$target,
           q[]                         => \$stdio);

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
$log->level($ALL);
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

$target ||= $DEFAULT_TARGET;
$zone   ||= $DEFAULT_ZONE;

# Setup iRODS
my $irods;
if ($dry_run) {
  $irods = WTSI::NPG::DriRODS->new;
}
else {
  $irods = WTSI::NPG::iRODS->new;
}

# Find data objects
my @data_objs;
if ($stdio) {
  binmode \*STDIN, 'encoding(UTF-8)';

  $log->info('Reading iRODS paths from STDIN');
  while (my $line = <>) {
    chomp $line;
    push @data_objs, $line;
  }
}

# query iRODS - find all annotated objects restricted by target
push @data_objs, _find_data_objects($target);


@data_objs = uniq sort @data_objs;
$log->info('Processing ', scalar @data_objs, ' data objects');

# Update metadata
my $num_updated = 0;

if (@data_objs) {
  my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;

  my @init_args = (mlwh_schema => $wh_schema);
  my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(@init_args);
  my $obj_factory  = WTSI::NPG::HTS::Illumina::Merged::DataObjectFactory->new
      (irods => $irods,);

  $num_updated = WTSI::NPG::HTS::Illumina::Merged::MetaUpdater->new
    (irods        => $irods,
     obj_factory  => $obj_factory,
     lims_factory => $lims_factory)->update_secondary_metadata(\@data_objs);
}

$log->info("Updated metadata on $num_updated files");

sub _find_data_objects {
  my ($target_type) = @_;

  # Find one run's annotated objects by target
  my @query = ([$TARGET => $target_type], [$COMPOSITION => q[%], 'like']);
  $log->info('iRODS query: ', pp(\@query));

  return $irods->find_objects_by_meta("/$zone", @query);
}

__END__

=head1 NAME

npg_update_hts_merged_metadata

=head1 SYNOPSIS

npg_update_hts_merged_metadata [--dry-run] [--logconf file]
  [--target-type type] [--verbose] [--zone name]

 Options:

  --debug       Enable debug level logging. Optional, defaults to false.
  --dry-run
  --dry_run     Enable dry-run mode. Propose metadata changes, but do not
                perform them. Optional, defaults to true.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --target-type    
  --target_type Restrict to one specific file type to update. Optional.
  --verbose     Print messages while processing. Optional.
  --zone        The iRODS zone in which to work. Optional, defaults to 'seq'.
  -             Read iRODS paths from STDIN instead of finding them by their
                run, lane and tag index.

=head1 DESCRIPTION

This script updates secondary metadata (i.e. LIMS-derived metadata,
not primary experimental metadata) on Illumina library merged data files 
in iRODS. The files may be specified by target type. Additionally a list 
of iRODS paths may be piped to STDIN.

In dry run mode, the proposed metadata changes will be written as INFO
notices to the log.

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
