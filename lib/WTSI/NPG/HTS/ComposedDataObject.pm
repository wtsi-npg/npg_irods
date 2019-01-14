package WTSI::NPG::HTS::ComposedDataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::iRODS::Metadata qw[$COMPOSITION];
use npg_tracking::glossary::composition::component::generic;
use npg_tracking::glossary::composition;

our $VERSION= '';

extends 'WTSI::NPG::HTS::DataObject';

has 'composition' =>
  (isa           => 'npg_tracking::glossary::composition',
   is            => 'rw',
   required      => 1,
   predicate     => 'has_composition',
   builder       => '_build_composition',
   lazy          => 1,
   documentation => 'The composition of the data contained in this file');

sub _build_composition {
  my ($self) = @_;

  my $path = $self->str;
  if (not $self->is_present) {
    $self->logconfess('Failed to build the composition attribute from the ',
                      "iRODS metadata of '$path' because the data object is ",
                      'not currently (or yet) stored in iRODS');
  }

  if (not $self->find_in_metadata($COMPOSITION)) {
    $self->logconfess('Failed to build the composition attribute from the ',
                      "iRODS metadata of '$path' because the data object is ",
                      "missing the '$COMPOSITION' AVU in iRODS");
  }

  my $json = $self->get_avu($COMPOSITION)->{value};

  my $class = 'npg_tracking::glossary::composition::component::generic';
  return npg_tracking::glossary::composition->thaw
    ($json, component_class => $class);
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ComposedDataObject

=head1 DESCRIPTION

A data object whose contents have been derived from one or more
sources, described by the value of its composition attribute.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

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
