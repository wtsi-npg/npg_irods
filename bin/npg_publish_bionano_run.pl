#!/usr/bin/env perl

use strict;
use warnings;

use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use Log::Log4perl qw(:levels);
use Pod::Usage;
use Try::Tiny;
use WTSI::DNAP::Utilities::Collector;
use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::OM::BioNano::Publisher;
use WTSI::NPG::OM::BioNano::ResultSet;

our $VERSION = '';
our $BIONANO_REGEX = qr{^\S+_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}$}msx;
our $DEFAULT_DAYS = 7;

if (! caller ) { run(); }
sub run {

    my $days;
    my $days_ago;
    my $debug;
    my $log4perl_config;
    my $publish_dest;
    my $source;
    my $verbose;

    GetOptions(
        'days=i'           => \$days,
        'days-ago=i'       => \$days_ago,
        'debug'            => \$debug,
        'dest=s'           => \$publish_dest,
        'help'             => sub { pod2usage(-verbose => 2,
                                              -exitval => 0) },
        'logconf=s'        => \$log4perl_config,
        'source=s'         => \$source,
        'verbose'          => \$verbose
    );

    if (! defined $source) {
        pod2usage(-msg     => "A --source argument is required\n",
                  -exitval => 2);
    }
    if (! defined $publish_dest) {
        pod2usage(-msg     => "A --dest argument is required\n",
                  -exitval => 2);
    }
    $days           ||= $DEFAULT_DAYS;
    $days_ago       ||= 0;

    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             levels => \@log_levels);
    my $log = Log::Log4perl->get_logger('main');

    my $now = DateTime->now;
    my $end;
    if ($days_ago > 0) {
        $end = DateTime->from_epoch
            (epoch => $now->epoch)->subtract(days => $days_ago);
    }
    else {
        $end = $now;
    }
    my $begin = DateTime->from_epoch
        (epoch => $end->epoch)->subtract(days => $days);

    my $source_dir = abs_path($source);
    $log->info(q[Publishing from '], $source_dir, q[' to '],
               $publish_dest, q[' BioNano results finished between ],
               $begin->iso8601, q[ and ], $end->iso8601);
    my $collector = WTSI::DNAP::Utilities::Collector->new(
        root  => $source_dir,
        depth => 2,
        regex => $BIONANO_REGEX,
    );
    my @dirs = $collector->collect_dirs_modified_between($begin->epoch,
                                                         $end->epoch);
    my $total = scalar @dirs;
    my $num_published = 0;

    $log->debug(q[Publishing ], $total, q[BioNano data directories in '],
                $source_dir, q[']);
    foreach my $dir (@dirs) {
        try {
            my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
                directory => $dir,
            );
            my $publisher = WTSI::NPG::OM::BioNano::Publisher->new(
                resultset => $resultset,
            );
            my $dest_collection = $publisher->publish($publish_dest);
            $num_published++;
            $log->info(q[Published BioNano run directory '], $dir,
                       q[' to iRODS collection '], $dest_collection,
                       q[': ], $num_published, q[ of ], $total);
        } catch {
            $log->error("Failed to publish '$dir': ", $_);
        };
    }
    $log->info("Finished; published $num_published runs of $total input");

    return 1;
}

__END__

=head1 NAME

npg_publish_bionano_run

=head1 SYNOPSIS

Options:

  --days-ago        The number of days ago that the publication window
                    ends. Optional, defaults to zero (the current day).
  --days            The number of days in the publication window, ending
                    at the day given by the --days-ago argument. Any sample
                    data modified during this period will be considered
                    for publication. Optional, defaults to 7 days.
  --dest            The data destination root collection in iRODS.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --source          The root directory to search for BioNano data.
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
