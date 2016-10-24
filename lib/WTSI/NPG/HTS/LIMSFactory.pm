package WTSI::NPG::HTS::LIMSFactory;

use List::AllUtils qw[any];
use Moose;
use MooseX::StrictConstructor;
use Scalar::Util qw[refaddr];

use npg_tracking::util::types qw[:all];
use st::api::lims;

our $VERSION = '';

with qw[WTSI::DNAP::Utilities::Loggable];

has 'mlwh_schema' =>
  (is            => 'rw',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 0,
   predicate     => 'has_mlwh_schema',
   documentation => 'A ML warehouse handle to obtain secondary metadata');

has 'driver_type' =>
  (is            => 'rw',
   isa           => 'Str',
   required      => 1,
   default       => 'ml_warehouse_fc_cache',
   documentation => 'The ML warehouse driver type used when obtaining ' .
                    'secondary metadata');

has '_lims_cache' =>
  (is       => 'rw',
   isa      => 'HashRef',
   required => 1,
   default  => sub { return {} });

=head2 positions

  Arg [1]      Run identifier, Int.

  Example    : my @positions = $factory->positions(17750)
  Description: Return the valid lane positions of a run, sorted in
               ascending order.
  Returntype : Array[Int]

=cut

sub positions {
  my ($self, $id_run) = @_;

  defined $id_run or
    $self->logconfess('A defined id_run argument is required');

  my $run = $self->make_lims($id_run);
  my @positions = sort map {$_->position} $run->children;

  return @positions;
}

=head2 tag_indices

  Arg [1]      Run identifier, Int.
  Arg [2]      Lane position, Int.

  Example    : my @tag_indices = $factory->tag_indices(17750, 1)
  Description: Return the valid tag indices of a lane, sorted in
               ascending order.
  Returntype : Array[Int]

=cut

sub tag_indices {
  my ($self, $id_run, $position) = @_;

  defined $id_run or
    $self->logconfess('A defined id_run argument is required');
  defined $position or
    $self->logconfess('A defined position argument is required');

  my $lane = $self->make_lims($id_run, $position);
  my @tag_indices = sort map {$_->tag_index} $lane->children;

  return @tag_indices;
}

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
  my ($self, $id_run, $position, $tag_index) = @_;

  defined $id_run or
    $self->logconfess('A defined id_run argument is required');

  $self->debug('Making a lims using driver_type ', $self->driver_type);

  if (not exists $self->_lims_cache->{$id_run}) {
    my $run;

    if ($self->has_mlwh_schema) {
      $run = st::api::lims->new(driver_type => $self->driver_type,
                                mlwh_schema => $self->mlwh_schema,
                                id_run      => $id_run);
    }
    else {
      $run = st::api::lims->new(driver_type => $self->driver_type,
                                id_run      => $id_run);
    }

    if ($run->can('mlwh_schema')) {
      if ($self->has_mlwh_schema) {
        # Sanity check that the handle used by the st::api::lims is
        # the same as any handle we have cached
        my $mlwh1 = $run->mlwh_schema;
        my $mlwh2 = $self->mlwh_schema;
        if (defined $mlwh1 and ref $mlwh1 and
            defined $mlwh2 and ref $mlwh2 and
            refaddr($mlwh1) != refaddr($mlwh2)) {
          $self->logconfess('The WTSI::DNAP::Warehouse::Schema cached by ',
                            'WTSI::NPG::HTS::LIMSFactory is not the same ',
                            "as that in the st::api::lims for run $id_run");
        }
      }
      else {
        # If the st::api::lims provided a database handle itself and the
        # factory has not, cache the handle.
        $self->mlwh_schema($run->mlwh_schema);
      }
    }

    $self->_lims_cache->{$id_run} = $run;
  }

  my $lims = $self->_lims_cache->{$id_run};
  defined $lims or
      $self->logconfess("Failed to create st::api::lims for run $id_run");

  $self->debug("Driver st::api::lims for run $id_run is ", $lims->driver);

  if (defined $position) {
    ($lims) = grep { $_->position == $position } $lims->children;
    defined $lims or
      $self->logconfess("Failed to create st::api::lims for run $id_run ",
                        "lane $position");
  }
  if ($tag_index) {
    ($lims) = grep { $_->tag_index == $tag_index } $lims->children;
    defined $lims or
      $self->logconfess("Failed to create st::api::lims for run $id_run ",
                        "lane $position tag_index $tag_index");
  }

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

The factory will also cache any WTSI::DNAP::Warehouse::Schema created
by the st::api::lims objects to enablke them to share the same
underlying database connection.

This functionality probably belongs in the st::api and could be moved
there, making this class redundant.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
