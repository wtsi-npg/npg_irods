package WTSI::NPG::HTS::PacBio::Sequel::Product;

use Moose;
use English qw[-no_match_vars];

with qw/WTSI::DNAP::Utilities::Loggable/;

our $VERSION = '';

my $ID_SCRIPT = q[generate_pac_bio_id];
my $ID_LENGTH = 64;

=head2 generate_product_id

  Arg [1]    : Run name, String. Required.
  Arg [2]    : Well label, String. Required.
  Arg [3]    : Comma separated list of tag sequences, String. Optional.
  Example    : $id = $self->generate_product_id($run, $well, $tags);
  Description: Runs a python script which generates a product id from run,
               well and tag data.

=cut

sub generate_product_id {
  my ($self, $run_name, $well_label, $tags) = @_;

  my $command = join q[ ],
    $ID_SCRIPT, '--run_name', $run_name, '--well_label', $well_label;
  foreach my $tag (@{$tags}){
    $command .= join q[ ], ' --tag', $tag;
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
