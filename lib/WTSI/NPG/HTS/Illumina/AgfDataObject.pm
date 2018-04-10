package WTSI::NPG::HTS::Illumina::AgfDataObject;

use namespace::autoclean;
use List::AllUtils qw[none];
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';

with qw[
         WTSI::NPG::HTS::AlFilter
         WTSI::NPG::HTS::Illumina::RunComponent
         WTSI::NPG::HTS::Illumina::FilenameParser
       ];

has '+is_restricted_access' =>
  (is => 'ro');

has '+primary_metadata' =>
  (is => 'ro');


sub BUILD {
  my ($self) = @_;

  my ($id_run, $position, $tag_index, $alignment_filter, $file_format) =
    $self->parse_file_name($self->str);

  if (not defined $self->id_run) {
    defined $id_run or
      $self->logconfess('Failed to parse id_run from path ', $self->str);
    $self->set_id_run($id_run);
  }
  if (not defined $self->position) {
    defined $position or
      $self->logconfess('Failed to parse position from path ', $self->str);
    $self->set_position($position);
  }
  if (defined $tag_index and not defined $self->tag_index) {
    $self->set_tag_index($tag_index);
  }

  if (not defined $self->alignment_filter) {
    $self->set_alignment_filter($alignment_filter);
  }

  # Modifying read-only attributes
  push @{$self->primary_metadata},$ID_RUN,$POSITION,$TAG_INDEX,$ALT_PROCESS,$GBS_PLEX_NAME;

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

WTSI::NPG::HTS::Illumina::AgfDataObject

=head1 DESCRIPTION

Represents Illumina genotype calling files in iRODS. This class 
overrides some base class behaviour to introduce:

 Custom metadata additions.

 Custom behaviour with respect to the file having restricted access.

=head1 AUTHOR

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
