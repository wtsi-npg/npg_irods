package WTSI::NPG::HTS::CompositionAnnotator;

use Data::Dump qw[pp];
use Moose::Role;

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

sub make_composition_metadata {
  my ($self, $composition) = @_;

  $composition or $self->logconfess('A composition argument is required');
  ref $composition or
    $self->logconfess('The composition argument must be a reference');

  my @avus;
  push @avus, $self->make_avu($COMPOSITION, $composition->freeze);
  push @avus, $self->make_avu($ID_PRODUCT, $composition->digest);
  foreach my $component ($composition->components_list) {
    push @avus, $self->make_component_metadata($component);
  }

  return @avus;
}

sub make_component_metadata {
  my ($self, $component) = @_;

  $component or $self->logconfess('A component argument is required');

  return ($self->make_avu($COMPONENT, $component->freeze));
}


no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::HTS::CompositionAnnotator

=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited.  All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
