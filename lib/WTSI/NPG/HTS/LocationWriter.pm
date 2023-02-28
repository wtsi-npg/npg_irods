package WTSI::NPG::HTS::LocationWriter;

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

Readonly::Scalar my $DELIMITER => "\0";
Readonly::Scalar my $JSON_FILE_VERSION => '1.0';
Readonly::Scalar my $NPG_PROD => 'npg-prod';

has 'path' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 1,
  documentation => 'The path to the output file');

has 'locations' => (
  isa           => 'HashRef',
  is            => 'rw',
  required      => 1,
  lazy          => 1,
  builder       => '_build_locations',
  documentation => 'A hash with keys built from collection and id_product, ' .
                    'and lists of locations as values');

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
  my $locations = {};
  if (-e $self->path) {
    open my $fh , '<:encoding(UTF-8)', $self->path or
      $self->logcroak(q[could not open ml warehouse json file] .
        $self->path);
    my $file_contents = <$fh>;
    close $fh or $self->logcroak(q[could not close ml warehouse ] .
        qq[json file $self->path]);

    try {
      my $decoded = $self->decode($file_contents);
      foreach my $product (@{$decoded->{products}}){
        my ($key, $paths) = $self->_build_location_pair(
          $product->{irods_root_collection},
          $product->{irods_data_relative_path},
          $product->{id_product},
          $product->{irods_secondary_data_relative_path}
          );
        $locations->{$key} = $paths

      }
      $self->debug('Read previous locations from file: ', $self->path);

    } catch{
      $self->logcroak('Failed to parse locations from JSON file: ', $self->path);
    }
  }else{
    $self->debug("No file at $self->{path}, using empty location arrayref");
    $locations = {};
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
                                   pid      => $pid);
  Description: Add a key value pair to the location hash, replacing any prior
               value
  Returntype : Void

=cut
sub add_location{
  my $positional = 1;
  my @named      = qw[pid coll path secondary_path];
  my $params     = function_params($positional, @named);

  my ($self) = $params->parse(@_);
  my $secondary_path = '';
  if ($params->secondary_path){
    $secondary_path = $params->secondary_path;
  }
  my ($key, $paths) = $self->_build_location_pair(
    $params->coll,
    $params->path,
    $params->pid,
    $secondary_path);
  $self->{locations}->{$key} = $paths;

  return;
}

sub _build_location_pair{

  my ($self, $coll, $path, $pid, $secondary_path) = @_;

  if ($coll !~ m{.*/$}xms){
    $coll .= q[/]
  }

  my $key = $coll . $DELIMITER . $pid;
  my $paths = $path;
  if ($secondary_path){
    $paths .= $DELIMITER . $secondary_path;
  }

  return $key, $paths;
}

=head2 write_locations

  Example    : $self->write_locations();
  Description: Write the locations file from this object.
  Returntype : Void

=cut
sub write_locations{
  my ($self) = @_;

  if (!%{$self->locations}){
    $self->warn('No irods locations to write');
    return;
  }

  my $json_out = {
    version  => $JSON_FILE_VERSION,
    products => {}
  };

  # Extract product rows from locations hash
  foreach my $key ( keys %{$self->locations}){
    my ($coll, $pid) = split $DELIMITER, $key;
    my ($path, $secondary_path) = split $DELIMITER, $self->locations->{$key};
    my $location = {
      irods_root_collection    => $coll,
      id_product               => $pid,
      irods_data_relative_path => $path,
      seq_platform_name        => $self->{platform_name},
      pipeline_name            => $self->{pipeline_name}
    };
    if ($secondary_path){
      $location->{irods_secondary_data_relative_path} = $secondary_path;
    }
    push @{$json_out->{products}}, $location;
  }

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

WTSI::NPG::HTS::LocationWriter

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
