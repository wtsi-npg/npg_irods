package WTSI::NPG::OM::BioNano::RunFinder;

use Moose;
use namespace::autoclean;

use Cwd qw[cwd abs_path];
use DateTime;
use WTSI::DNAP::Utilities::Collector;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';
our $BIONANO_REGEX = qr{^\S+_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}$}msx;
our $DEFAULT_DAYS = 7;
our $DEFAULT_DEPTH = 2;

has 'search_depth' =>
  (is       => 'ro',
   isa      => 'Int',
   lazy     => 1,
   default  => $DEFAULT_DEPTH,
   documentation => 'Depth of search for BioNano runfolders',
);


=head2 find

  Arg [1]    : [Str] Path of directory to search
  Arg [2]    : [Int] Number of days ago that the publication window ends.
               Optional, defaults to zero.
  Arg [3]    : [Int] Length in days of the publication window.
               Optional, defaults to 7.

  Example    : $finder->find('/foo')
  Description: Search for BioNano runfolders with the given parameters.
  Returntype : Array[Str]

=cut

sub find {

    my ($self, $search_dir, $days_ago, $days) = @_;

    $search_dir ||= cwd();
    $search_dir = abs_path($search_dir);
    $days_ago ||= 0;
    $days ||= $DEFAULT_DAYS;

    my $now = DateTime->now;
    my $end;
    if ($days_ago > 0) {
        $end = DateTime->from_epoch
            (epoch => $now->epoch)->subtract(days => $days_ago);
    } else {
        $end = $now;
    }
    my $begin = DateTime->from_epoch
        (epoch => $end->epoch)->subtract(days => $days);
    $self->info(q[Searching directory '], $search_dir,
                q[' for BioNano results finished between ],
                $begin->iso8601, q[ and ], $end->iso8601);
    my $collector = WTSI::DNAP::Utilities::Collector->new(
        root  => $search_dir,
        depth => $self->search_depth,
        regex => $BIONANO_REGEX,
    );
    my @dirs = $collector->collect_dirs_modified_between($begin->epoch,
                                                         $end->epoch);
    $self->info(q[Found ], scalar @dirs, q[ BioNano runfolders]);
    return @dirs;

}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::RunFinder

=head1 SYNOPSIS

my $finder = WTSI::NPG::OM::BioNano::RunFinder->new(search_depth => $depth);

my @dirs = $finder->find($search_dir, $days_ago, $days);

=head1 DESCRIPTION

Class to find BioNano runs in a given target directory.

Each runfolder name must be of the form barcode_timestamp, where
'barcode' may contain any non-whitespace characters and 'timestamp'
is of the form yyy-mm-dd_HH_MM.

Maximum search depth defaults to 2.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

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
