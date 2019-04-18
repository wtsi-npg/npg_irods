package WTSI::NPG::HTS::10X::DataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::ComposedDataObject';

has '+is_restricted_access' =>
  (is => 'ro');

has '+primary_metadata' =>
  (is => 'ro');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata},
    $COMPONENT,
    $COMPOSITION,
    $ID_RUN,
    $POSITION,
    $TAG_INDEX,
    $TARGET;

  # Restricted access files will get some secondary metadata, limited
  # to $STUDY_ID
  if ($self->is_restricted_access) {
    $self->secondary_metadata([$STUDY_ID]);
  }

  return;
}

sub _build_is_restricted_access {
  my ($self) = @_;

  return 1;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME



=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
