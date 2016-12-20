package WTSI::NPG::HTS::10x::FastqDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[none];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';

with qw[
         WTSI::NPG::HTS::AlFilter
         WTSI::NPG::HTS::10x::RunComponent
         WTSI::NPG::HTS::10x::FilenameParser
       ];

has '+primary_metadata' =>
  (is => 'ro');

sub BUILD {
  my ($self) = @_;

  my ($read, $tag, $position, $file_format) =
    $self->parse_file_name($self->str);

  if (defined $read and not defined $self->read) {
    $self->set_read($read);
  }
  if (not defined $self->position) {
    defined $position or
      $self->logconfess('Failed to parse position from path ', $self->str);
    $self->set_position($position);
  }
  if (defined $tag and not defined $self->tag) {
    $self->set_tag($tag);
  }

  # Modifying read-only attribute
  push @{$self->primary_metadata},
    $ALT_PROCESS,
    $ID_RUN,
    $POSITION,
    $READ,
    $TAG;

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10x::FastqDataObject

=head1 DESCRIPTION

Represents 10x fastq files in iRODS. This class overrides
some base class behaviour to introduce:

 Custom primary metadata restrictions.

 Custom secondary metadata restrictions.

 Custom behaviour with respect to the file having restricted access
 (i.e. for BED and JSON files only).

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
