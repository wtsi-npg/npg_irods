package WTSI::NPG::HTS::Illumina::IndexDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[none];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::Illumina::DataObject';

has '+is_restricted_access' =>
  (is => 'ro');

has '+primary_metadata' =>
  (is => 'ro');

has 'indexed_object' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   builder       => '_build_indexed_object',
   lazy          => 1,
   documentation => 'The absolute path to the data object this index '.
                    'describes');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata}, $ALT_PROCESS;

  return;
}

override 'update_secondary_metadata' => sub {
  my ($self, @avus) = @_;

  # Nothing to add

  # No attributes, none processed, no errors
  return (0, 0, 0);
};

sub _build_indexed_object {
  my ($self) = @_;

  my $path;
  if ($self->file_format eq 'crai') {
    ($path) = $self->str =~ qr{(.*)[.]crai$}mxs;
  }
  elsif ($self->file_format eq 'bai') {
    ($path) = $self->str =~ qr{(.*)[.]bai$}mxs;
  }
  elsif ($self->file_format eq 'pbi') {
    ($path) = $self->str =~ qr{(.*)[.]pbi$}mxs;
  }
  else {
    $self->logcroak('Failed to find the indexed file for ', $self->str,
                    '. Unknown index type ', $self->file_format);
  }

  return $path;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::IndexDataObject

=head1 DESCRIPTION

Represents an index associated with another file in iRODS e.g. a cram
file. This class overrides some base class behaviour to introduce:

 Custom primary metadata restrictions.

 Custom behaviour with respect to the file having restricted access
 (i.e. never).

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

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
