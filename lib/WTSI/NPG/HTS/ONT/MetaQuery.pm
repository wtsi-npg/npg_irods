package WTSI::NPG::HTS::ONT::MetaQuery;

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

=head2 find_oseq_flowcells

  Arg [1]    : GridION experiment name, Str.
  Arg [2]    : GridION device ID, Str. E.g. 'GA10000'. Optional.

  Example    : @flowcell_records - $obj->find_oseq_flowcells($id, 'GA10000');
  Description: Returns flowcell records for a GridION experiment run.
               Pre-fetches related sample and study information.
  Returntype : Array[WTSI::DNAP::Warehouse::Schema::Result::OseqFlowcell]

=cut

sub find_oseq_flowcells {
  my ($self, $experiment_name, $device_id) = @_;

  defined $experiment_name or
    $self->logconfess('A defined experiment_name argument is required');
  defined $device_id or
    $self->logconfess('A defined device_id argument is required');

  my %slot_lookup = ('GA10000' => 1,
                     'GA20000' => 2,
                     'GA30000' => 3,
                     'GA40000' => 4,
                     'GA50000' => 5);
  my $slot = $slot_lookup{$device_id};

  defined $slot or
    $self->logconfess("Invalid device_id '$device_id'");

  my $query = {experiment_name => $experiment_name};
  if (defined $device_id) {
    $query->{instrument_slot} = $slot;
  }

  my @flowcell_records = $self->mlwh_schema->resultset('OseqFlowcell')->search
    ($query, {prefetch => ['sample', 'study']});
  my $num_records = scalar @flowcell_records;

  $self->debug(sprintf q[Found %d flowcell records for experiment '%s' ] .
               q[device_id %s], $num_records, $experiment_name,
               defined $device_id ? $device_id : 'undef');

  # If a device_id is supplied, there should be only a single record
  if (defined $device_id and $num_records > 1) {
    $self->logcroak("ML warehouse returned $num_records records for ",
                    "experiment '$experiment_name' device_id '$device_id'");
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

WTSI::NPG::HTS::ONT::MetaQuery

=head1 DESCRIPTION

Queries WTSI::DNAP::Warehouse::Schema for secondary metadata in order
to update ONT data files in iRODS.

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
