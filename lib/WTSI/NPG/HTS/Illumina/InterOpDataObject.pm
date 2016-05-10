package WTSI::NPG::HTS::Illumina::InterOpDataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::iRODS::Metadata qw[$ID_RUN];

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';

with 'WTSI::NPG::HTS::Illumina::RunComponent';

has '+is_restricted_access' =>
  (is => 'ro');

has '+primary_metadata' =>
  (is => 'ro');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata}, $ID_RUN;

  return;
}

override 'update_secondary_metadata' => sub {
  my ($self) = @_;

  # Nothing to add

  return $self;
};

sub _build_is_restricted_access {
  my ($self) = @_;

  return 0;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::InterOpDataObject

=head1 DESCRIPTION

Represents InterOp .bin files in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
