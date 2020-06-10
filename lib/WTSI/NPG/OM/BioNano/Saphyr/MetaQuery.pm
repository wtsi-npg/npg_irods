package WTSI::NPG::OM::BioNano::Saphyr::MetaQuery;

use namespace::autoclean;
use File::Basename;
use Moose::Role;
use MooseX::StrictConstructor;

use WTSI::DNAP::Warehouse::Schema;

with qw[
    WTSI::DNAP::Utilities::Loggable
];

our $VERSION = '';

has 'mlwh_schema' =>
    (is            => 'ro',
     isa           => 'WTSI::DNAP::Warehouse::Schema',
     required      => 1,
     lazy          => 1,
     builder       => '_build_mlwh_schema',
     documentation => 'A ML warehouse handle to obtain secondary metadata');

=head2 find_bmap_flowcells

  Arg [1]    : Chip serial number, Str.
  Arg [2]    : Flowcell position, Int. Optional

  Example    : @flowcell_record = $obj->find_bmap_flowcell($serial, 1);
  Description: Returns the flowcell records for a

               Pre-fetches related sample and study information.
  Returntype : Array[WTSI::DNAP::Warehouse::Schema::Result::BmapFlowcell]

=cut

sub find_bmap_flowcells {
  my ($self, $chip_serialnumber, $position) = @_;

  defined $chip_serialnumber or
      $self->logconfess('A defined chip_serialnumber argument is required');
  defined $position or
      $self->logconfess('A defined position argument is required');
  $position =~ m{[12]}msx or
      $self->logcroak("Invalid flowcell position '$position': expected ",
                      'one of [1, 2]');

  my $query = {chip_serialnumber => $chip_serialnumber};
  if (defined $position) {
    $query->{position} = $position;
  }

  my @flowcell_records = $self->mlwh_schema->resultset('BmapFlowcell')->search
      ($query, {prefetch => ['sample', 'study']});
  my $num_records = scalar @flowcell_records;

  $self->debug(sprintf q[Found %d flowcell records for chip '%s' ] .
                       q[position %s], $num_records, $chip_serialnumber,
                       defined $position ? $position : 'undef');

  # If a position is supplied, there should be only a single record
  if (defined $position and $num_records > 1) {
    $self->logcroak("ML warehouse returned $num_records records for ",
                    "chip '$chip_serialnumber' position '$position'");
  }

  return @flowcell_records;
}

sub _build_mlwh_schema {
  my ($self) = @_;

  return WTSI::DNAP::Warehouse::Schema->connect;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::MetaQuery

=head1 DESCRIPTION

Queries WTSI::DNAP::Warehouse::Schema for secondary metadata in order
to update BioNano Saphyr data files in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
