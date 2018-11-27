package WTSI::NPG::HTS::Illumina::CompositionFileParser;

use namespace::autoclean;
use File::Basename;
use File::Slurp;
use JSON;
use Moose::Role;

use npg_tracking::glossary::composition::component::illumina;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

# Extract the "product name"
sub parse_composition_filename {
  my ($self, $file_path) = @_;

  $file_path or
    $self->logconfess('A non-empty file_path argument is required');

  $file_path =~ m{[.]composition[.]json$}msx or
    $self->logcroak("Path '$file_path' does not have the correct file name ",
                    'to be a composition file');

  return fileparse($file_path, '.composition.json');
}

sub read_composition_file {
  my ($self, $file_path) = @_;

  my $json = read_file($file_path, binmode => ':utf8');
  if (not $json) {
    $self->logcroak("Invalid composition file '$file_path': file is empty");
  }

  return $self->make_composition($json);
}

sub make_composition {
  my ($self, $str) = @_;

  if (not $str) {
    $self->logcroak('Failed to make a composition from an empty string');
  }

  return npg_tracking::glossary::composition->thaw
    ($str, component_class =>
     'npg_tracking::glossary::composition::component::illumina');
}

no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::CompositionFileParser

=head1 DESCRIPTION

A role providing utility methods for parsing composition file names
and content.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
