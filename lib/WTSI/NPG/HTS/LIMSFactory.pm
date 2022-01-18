package WTSI::NPG::HTS::LIMSFactory;

use Cache::LRU;
use List::AllUtils qw[any];
use Moose;
use MooseX::StrictConstructor;
use Scalar::Util qw[refaddr];

use npg_tracking::util::types qw[:all];
use st::api::lims;
use WTSI::DNAP::Warehouse::Schema;

our $VERSION = '';

with qw[WTSI::DNAP::Utilities::Loggable];

has 'mlwh_schema' =>
  (is            => 'rw',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 0,
   predicate     => 'has_mlwh_schema',
   clearer       => 'clear_mlwh_schema',
   lazy          => 1,
   default       => sub {return WTSI::DNAP::Warehouse::Schema->connect();},
   documentation => 'A ML warehouse handle to obtain secondary metadata');

has 'driver_type' =>
  (is            => 'rw',
   isa           => 'Str',
   required      => 1,
   default       => 'ml_warehouse_fc_cache',
   documentation => 'The ML warehouse driver type used when obtaining ' .
                    'secondary metadata');

has 'max_lims_cache_size' =>
    (is            => 'ro',
     isa           => 'Int',
     required      => 1,
     default       => 100,
     documentation => 'The maximum number of lims objects to cache during ' .
                      'operation');

has 'lims_cache' =>
  (is            => 'ro',
   isa           => 'Cache::LRU',
   required      => 1,
   lazy          => 1,
   default       => sub {
     my ($self) = @_;
     return Cache::LRU->new(size => $self->max_lims_cache_size)
   },
   documentation => 'Cache of st::api::lims indexed on rpt list',
   init_arg      => undef);


=head2 make_lims

  Arg [1]      Run identifier, Int.
  Arg [2]      Lane position, Int. Optional.
  Arg [3]      Tag index, Int. Optional.

  Example    : my $lims = $factory->make_lims(17750, 1, 0)
  Description: Return an st::api::lims for the specified run
               (and possibly lane, plex).
  Returntype : st::api::lims

=cut

sub make_lims {
  my ($self, $composition) = @_;

  $composition or $self->logconfess('A composition argument is required');

  $self->debug('Making a lims using driver_type ', $self->driver_type);

  my $rpt = $composition->freeze2rpt;
  if (exists $self->lims_cache->{$rpt}) {

    $self->debug("Using cached LIMS for '$rpt'");
    return $self->lims_cache->{$rpt};
  }

  my @init_args = (driver_type => $self->driver_type,
                   rpt_list    => $rpt);
  if ($self->driver_type =~ /^ml_warehouse/xms) {
    push @init_args, mlwh_schema => $self->mlwh_schema;
  }

  my $lims = st::api::lims->new(@init_args);

  $self->lims_cache->{$rpt} = $lims;

  return $lims;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::LIMSFactory

=head1 DESCRIPTION

A factory for creating st::api::lims objects given run, lane and plex
information. This class exists only to encapsulate the ML warehouse
queries and driver creation necessary to make st::api::lims
objects. It will serve as a cache for these objects, if required.

The factory will cache an st::api::lims objects for each rpt list it
encounters. The factory will also cache any
WTSI::DNAP::Warehouse::Schema created by the st::api::lims objects to
enable them to share the same underlying database connection.

N.B. The st::api::lims objects are constructed per-rpt list; no
account is taken of subset and these object are unsuitable for doing
queries on data which vary by subset.


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2019 Genome Research Limited. All Rights
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
