package WTSI::NPG::HTS::WriteLocations;

use namespace::autoclean;

use Data::Dump qw[pp];
use English qw[-no_match_vars];
use Moose;
use Readonly;
use Try::Tiny;
use WTSI::DNAP::Utilities::Params qw[function_params];


with qw[
  WTSI::DNAP::Utilities::Loggable
  WTSI::DNAP::Utilities::JSONCodec
];

our $VERSION = '';

Readonly::Scalar my $JSON_FILE_VERSION => '1.0';
Readonly::Scalar my $NPG_PROD => 'npg-prod';

has 'path' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 1,
  documentation => 'The path to the output file');

has 'locations' => (
  isa           => 'ArrayRef',
  is            => 'rw',
  required      => 1,
  lazy          => 1,
  builder       => '_build_locations',
  documentation => 'The rows of the seq_product_irods_locations table for each' .
                   'target data object loaded');

has 'pipeline_name' =>(
  isa           => 'Str',
  is            => 'ro',
  required      => 1,
  default       => $NPG_PROD,
  documentation => 'The name of the pipeline used to produce the data');

has 'platform_name' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 1,
  documentation => 'Name of the platform used to produce raw data');

sub _build_locations {
  my ($self) = @_;
  my $locations;
  if (-e $self->path) {
    open my $fh , '<:encoding(UTF-8)', $self->path or
      $self->logcroak(q[could not open ml warehouse json file] .
        $self->path);
    my $file_contents = <$fh>;
    close $fh or $self->logcroak(q[could not close ml warehouse ] .
        qq[json file $self->path]);

    try {
      my $decoded = $self->decode($file_contents);
      $locations = $decoded->{products};
      $self->debug('Read previous locations from file: ', $self->path);

    } catch{
      $self->logcroak('Failed to parse locations from JSON file: ', $self->path);
    }
  }else{
    $self->debug("No file at $self->{path}, using empty location arrayref");
    $locations = [];
  }
  return $locations;

}

=head2 add_location

  Named args : coll                 Collection. Str.
               path                 Relative Path. Str.
               pid                  Product Id. Str.
               secondary_path       Path to secondary data object for this product
                                    Str. Optional.

  Example    : $self->add_location(coll     => $collection,
                                   path     => $path,
                                   pid      => $pid,
  Description: Add a data hash to the location array, if it is not already
               present
  Returntype : Void

=cut
sub add_location{
  my $positional = 1;
  my @named      = qw[pid coll path secondary_path];
  my $params = function_params($positional, @named);

  my ($self) = $params->parse(@_);

  my $coll = $params->coll;

  if ($coll !~ m{.*/$}xms){
    $coll .= q[/]
  }

  # Get a list of all entries that do not have the same pid and coll as the
  # one to be added
  my @existing = grep {
    $params->pid ne $_->{id_product} ||
      $params->coll ne $_->{irods_root_collection}}
    @{$self->locations};

  my $location = {
    id_product               => $params->pid,
    seq_platform_name        => $self->{platform_name},
    pipeline_name            => $self->{pipeline_name},
    irods_root_collection    => $coll,
    irods_data_relative_path => $params->path
  };
  if ($params->secondary_path) {
    $location->{irods_secondary_data_relative_path} = $params->secondary_path;
  }

  if (scalar (@existing) < scalar @{$self->locations}){
    $self->{locations} = \@existing;
  }

  $self->debug('Adding product location, ', pp($location));
  push @{$self->{locations}}, $location;

  return;

}

=head2 write_locations

  Example    : $self->write_locations();
  Description: Write the locations file from this object.
  Returntype : Void

=cut
sub write_locations{
  my ($self) = @_;

  if (@{$self->locations} == 0){
    $self->warn('No irods locations to write');
    return;
  }

  my $json_out = {
    version  => $JSON_FILE_VERSION,
    products => $self->locations
  };
  $self->info("Writing locations file '$self->{path}':", pp($json_out));

  open my $fh, '>:encoding(UTF-8)', $self->path or
    $self->logcroak(qq[Could not open ml warehouse json file $self->{path} ],
      q[to write]);
  print $fh $self->encode($json_out) or
    $self->logcroak(q[Could not write to ml warehouse json file ],
      $self->path);
  close $fh or $self->logcroak(q[Could not close ml warehouse json file ],
    $self->path);

  return;

}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::WriteLocations

=head1 DESCRIPTION

Stores information used to load the ml warehouse seq_product_irods_locations
table, and provides methods to write that information to a json file.

=head1 AUTHOR

Michael Kubiak E<lt>mk35@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
