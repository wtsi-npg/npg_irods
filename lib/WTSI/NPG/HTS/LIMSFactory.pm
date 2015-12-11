package WTSI::NPG::HTS::LIMSFactory;

use List::AllUtils qw(any);
use Moose;

use npg_tracking::util::types qw(:all);
use st::api::lims;
use st::api::lims::ml_warehouse;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'mlwh_schema' =>
  (is            => 'ro',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 1,
   documentation => 'A ML warehouse handle to obtain secondary metadata');

sub positions {
  my ($self, $id_run) = @_;

  defined $id_run or
    $self->logconfess('A defined id_run argument is required');

  my $run = $self->make_lims($id_run);
  my @positions = sort map {$_->position} $run->children;

  return @positions;
}

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
  Arg [2]      Lane position, Int.
  Arg [3]      Tag index, Int. Optional.

  Example    : my $lims = $factory->make_lims(17750, 1, 0)
  Description: Return a new st::api::lims for the specified run,
               lane (and possibly plex).
  Returntype : st::api::lims

=cut

sub make_lims {
  my ($self, $id_run, $position, $tag_index) = @_;

  defined $id_run or
    $self->logconfess('A defined id_run argument is required');

  my $flowcell = $self->_find_flowcell($id_run);

  my @initargs = (flowcell_barcode => $flowcell->flowcell_barcode,
                  id_flowcell_lims => $flowcell->id_flowcell_lims,
                  id_run           => $id_run);

  if (defined $position) {
    push @initargs, position => $position;
  }
  if (defined $tag_index) {
    push @initargs, tag_index => $tag_index;
  }

  my $driver = st::api::lims::ml_warehouse->new
    (mlwh_schema => $self->mlwh_schema, @initargs);

  return st::api::lims->new(driver => $driver, @initargs);
}

sub _find_flowcell {
  my ($self, $id_run) = @_;

  my $flowcells = $self->mlwh_schema->resultset('IseqFlowcell')->search
    ({'iseq_product_metrics.id_run' => $id_run},
     {join     => 'iseq_product_metrics',
      select   => ['flowcell_barcode', 'id_flowcell_lims'],
      distinct => 1});

  my @flowcells;
  while (my $fc = $flowcells->next) {
    push @flowcells, $fc;
  }

  my $num_flowcells = scalar @flowcells;
  if ($num_flowcells == 0) {
    $self->logconfess("LIMS returned no flowcells for run '$id_run'");
  }
  elsif ($num_flowcells > 1) {
    $self->logconfess("LIMS returned >1 ($num_flowcells) flowcells for ",
                      "run $id_run: ", pp(\@flowcells));
  }

  return shift @flowcells
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

This functionality probably belongs in the st::api and could be moved
there, making this class redundant.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
