package WTSI::NPG::HTS::WriteLocations;

use namespace::autoclean;

use Data::Dump qw[pp];
use English qw[-no_match_vars];
use Moose;
use Try::Tiny;
use WTSI::DNAP::Utilities::Params;


with qw[
  WTSI::DNAP::Utilities::Loggable
  WTSI::DNAP::Utilities::JSONCodec
];

our $VERSION = '';

Readonly::Scalar my $JSON_FILE_VERSION => '1.0';
Readonly::Scalar my $NPG_PROD => 'npg-prod';

has 'file' => {
  isa           => 'str',
  is            => 'ro',
  required      => 1,
  documentation => 'The path to the output file'
};

has 'locations' => {
  isa           => 'ArrayRef',
  is            => 'rw',
  required      => 1,
  builder       => '_build_locations',
  lazy          => 1,
  documentation => 'The rows of the seq_product_irods_locations table for each
                    target data object loaded'
};

sub _build_locations {
  my ($self) = @_;

  if (-e $self->file) {
    open my $fh , '<:encoding(UTF-8)', $self->file or
      $self->logcroak(q[could not open ml warehouse json file] .
        qq[$self->file_path]);
    my $file_contents = <$fh>;
    close $fh or $self->logcroak(q[could not close ml warehouse ] .
        qq[json file $self->mlwh_json]);

    try {
      my $decoded = $self->decode($file_contents);
      my $locations = $decoded->{products};
      $self->locations($locations);
      $self->debug('Read previous locations from file: ', $self->file);
    } catch{
      $self->logcroak('Failed to parse locations from JSON file: ', $self->file);
    }
  } else {
    $self->locations([]);
    $self->debug("No file at ${$self->file}, using empty location arrayref");
  }

}

=head2 add_location

  Named args : coll                 Collection. Str.
               path                 Relative Path. Str.
               pid                  Product Id. Str.
               platform             Sequencing Platform. Str.
               process              Process name. Str. Optional
                                    Defaults to 'npg-prod'.
               secondary_path       Path to secondary data object for this product
                                    Str. Optional.

  Example    : $self->add_location(coll     => $collection,
                                   path     => $path,
                                   pid      => $pid,
                                   platform => $platform)
  Description: Add a data hash to the location array, if it is not already
               present
  Returntype : Void

=cut
sub add_location{
  my $positional = 1;
  my @named      = qw[pid, platform, process, coll, path, secondary_path];
  my $params = function_params($positional, @named);

  my ($self) = $params->parse();

  my @existing = grep {
    $params->pid ne $_->{id_product} ||
      $params->coll ne $_->{irods_root_collection}}
    @{$self->{locations}};

  my $location = {
    id_product               => $params->pid,
    seq_platform_name        => $params->platform,
    pipeline_name            => defined($params->process) ?
      $params->process : $NPG_PROD,
    irods_root_collection    => $params->coll,
    irods_data_relative_path => $params->path
  };
  if ($params->secondary_path) {
    $location->{"irods_secondary_data_relative_path"} = $params->secondary_path;
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

  $self->debug("Writing to locations file '$self->{file}:'", pp($self->{locations}));

  my $json_out = {
    version  => $JSON_FILE_VERSION,
    products => $self->{locations}
  };

  open my $fh, '>:encoding(UTF-8)', $self->{file} or
    $self->logcroak(qq[Could not open ml warehouse json file $self->{file}],
      q[to write]);
  print $fh $self->encode($json_out) or
    $self->logcroak(q[Could not write to ml warehouse json file],
      qq[$self->{file}]);
  close $fh or $self->logcroak(q[Could not close ml warehouse json file],
    qq[$self->{file}]);

  return;

}


no Moose::Role;

1;

__END__
