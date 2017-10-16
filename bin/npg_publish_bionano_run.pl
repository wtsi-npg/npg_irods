#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;
use Try::Tiny;
use WTSI::DNAP::Utilities::ConfigureLogger qw[log_init];
use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::OM::BioNano::RunFinder;
use WTSI::NPG::OM::BioNano::RunPublisher;

our $VERSION = '';

if (! caller ) {
    my $result = run();
    if ($result == 0) { exit 1; }
    else { exit 0; }
}
sub run {

    my $days;
    my $days_ago;
    my $debug;
    my $log4perl_config;
    my $collection;
    my $runfolder_path;
    my $search_dir;
    my $verbose;

    GetOptions(
        'days=i'                          => \$days,
        'days-ago|days_ago=i'             => \$days_ago,
        'debug'                           => \$debug,
        'collection=s'                    => \$collection,
        'help'                            => sub {
            pod2usage(-verbose => 2, -exitval => 0) },
        'logconf=s'                       => \$log4perl_config,
        'runfolder-path|runfolder_path=s' => \$runfolder_path,
        'search-dir|search_dir=s'         => \$search_dir,
        'verbose'                         => \$verbose
    );

    if (defined $search_dir && defined $runfolder_path) {
        my $msg = "Cannot supply both --search_dir and --runfolder_path\n";
        pod2usage(-msg     => $msg,
                  -exitval => 2);
    }
    if (! defined $collection) {
        pod2usage(-msg     => "A --collection argument is required\n",
                  -exitval => 2);
    }

    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             levels => \@log_levels);
    my $log = Log::Log4perl->get_logger('main');

    # find and publish directories
    my @dirs;
    if (defined $runfolder_path) {
        push @dirs, $runfolder_path;
        $log->info(q[Publishing runfolder path '], $runfolder_path,
                   q[' to '], $collection, q[']);
    } else {
        my $finder = WTSI::NPG::OM::BioNano::RunFinder->new;
        @dirs = $finder->find($search_dir, $days_ago, $days);
    }

    my $irods = WTSI::NPG::iRODS->new;
    my $total = scalar @dirs;
    my $num_published = 0;
    my $errors = 0;
    $log->debug(q[Ready to publish ], $total, q[ BioNano runfolder(s) to '],
                $collection, q[']);
    my $wh_schema = WTSI::DNAP::Warehouse::Schema->connect;
    foreach my $dir (@dirs) {
        try {
          my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new
            (directory => $dir,
             mlwh_schema => $wh_schema,
             irods     => $irods);
            my $dest_collection = $publisher->publish($collection);
            $num_published++;
            $log->info(q[Published BioNano run directory '], $dir,
                       q[' to iRODS collection '], $dest_collection,
                       q[': ], $total, q[ runs attempted, ], $num_published,
                       q[ successes, ], $errors, q[ errors]);
        } catch {
            $log->error("Error publishing '$dir': ", $_);
            $errors++;
        };
    }
    my $status = 1;
    if ($errors == 0) {
        $log->info(q[Finished successfully: Published ],
                   $total, q[ BioNano run(s)]);
    } else {
        $log->error(q[Finished with errors: Attempted to publish ], $total,
                    q[ BioNano run(s); ], $num_published, q[ succeeded, ],
                    $errors, q[ failed]);
        $status = 0;
    }
    return $status;
}

__END__

=head1 NAME

npg_publish_bionano_run

=head1 SYNOPSIS

Options:

  --days-ago
  --days_ago        The number of days ago that the publication window
                    ends. Optional, defaults to zero (the current day).
                    Has no effect if the --runfolder_path option is used.
  --days            The number of days in the publication window, ending
                    at the day given by the --days-ago argument. Any sample
                    data modified during this period will be considered
                    for publication. Optional, defaults to 7 days.
                    Has no effect if the --runfolder_path option is used.
  --collection      The data destination root collection in iRODS.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --runfolder-path
  --runfolder_path  The instrument runfolder path to load. Incompatible
                    with --search_dir. Optional. If neither this option
                    nor --search_dir is given, the default value of
                    --search_dir is used.
  --search-dir
  --search_dir      The root directory to search for BioNano data. The
                    --days_ago and --days options determine a time window
                    for runfolders to be published. Incompatible with
                    --runfolder_path. Optional, defaults to current working
                    directory.
  --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

This script loads data and metadata for a unit BioNano runfolder into
iRODS. The 'unit' runfolder contains results for a run with one sample on
one flowcell. Typically, multiple unit runfolders are merged together for
downstream analysis.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

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
