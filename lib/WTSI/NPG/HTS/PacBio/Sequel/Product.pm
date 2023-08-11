package WTSI::NPG::HTS::PacBio::Sequel::Product;

use Moose;
use English qw[-no_match_vars];
use WTSI::DNAP::Utilities::Params qw[function_params];

with qw/WTSI::DNAP::Utilities::Loggable/;

our $VERSION = '';

my $ID_SCRIPT = q[generate_pac_bio_id];
my $ID_LENGTH = 64;

=head2 generate_product_id

  Arg [1]    : Run name, String. Required.
  Arg [2]    : Well label, String. Required.
  
  Named args : tags           Comma separated list of tag sequences, String.
               plate_number   Plate number (only relevant for Revio runs), Int.

  Example    : $id = $self->generate_product_id($run, $well, tags => $tags);
  Description: Runs a python script which generates a product id from run,
               well and tag data.

=cut

{
  my $positional = 3;
  my @named      = qw[tags plate_number];

  my $params     = function_params($positional, @named);

  sub generate_product_id {
    my ($self, $run_name, $well_label) = $params->parse(@_);

    defined $run_name or
      $self->logconfess('A defined run name argument is required');

    defined $well_label or
      $self->logconfess('A defined well label argument is required');

    my $command = join q[ ],
      $ID_SCRIPT, '--run_name', $run_name, '--well_label', $well_label;

    if (defined $params->tags) {
      foreach my $tag (@{$params->tags}){
        $command .= join q[ ], ' --tag', $tag;
      }
    }
    if (defined $params->plate_number) {
      $command .= ' --plate_number '. $params->plate_number;
    }
    $self->info("Generating product id: $command");

    open my $id_product_script, q[-|], $command
      or $self->logconfess('Cannot generate id_product ' . $CHILD_ERROR);
    my $id_product = <$id_product_script>;
    close $id_product_script
      or $self->logconfess('Could not close id_product generation script');
    $id_product =~ s/\s//xms;
    if (length $id_product != $ID_LENGTH) {
      $self->logcroak('Incorrect output length from id_product generation script, expected a 64 character string');
    }
    return $id_product;
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::Product

=head1 DESCRIPTION

Provides product data related methods.

=head1 AUTHOR

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
