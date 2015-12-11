package WTSI::NPG::HTS::AncFileDataObject;

use namespace::autoclean;
use Moose;
use Try::Tiny;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::DataObject';

with 'WTSI::NPG::HTS::RunComponent', 'WTSI::NPG::HTS::FilenameParser',
  'WTSI::NPG::HTS::Annotator';

has 'align_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   writer        => '_set_align_filter',
   documentation => 'The align filter, parsed from the iRODS path');

has 'file_format' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   writer        => '_set_file_format',
   documentation => 'The storage format of the file');

has '+id_run' =>
  (writer        => '_set_id_run',
   documentation => 'The run ID, parsed from the iRODS path');

has '+position' =>
  (writer        => '_set_position',
   documentation => 'The position (i.e. sequencing lane), parsed ' .
                    'from the iRODS path');

has '+tag_index' =>
  (writer        => '_set_tag_index',
   documentation => 'The tag_index, parsed from the iRODS path');

sub BUILD {
  my ($self) = @_;

  my ($id_run, $position, $tag_index, $align_filter, $file_format) =
    $self->parse_file_name($self->str);

  if (not defined $self->id_run) {
    defined $id_run or
      $self->logconfess('Failed to parse id_run from path ', $self->str);
    $self->_set_id_run($id_run);
  }

  if (not defined $self->position) {
    defined $position or
      $self->logconfess('Failed to parse position from path ', $self->str);
    $self->_set_position($position);
  }

  if (not defined $self->file_format) {
    defined $file_format or
      $self->logconfess('Failed to parse file format from path ', $self->str);
    $self->_set_file_format($file_format);
  }

  if (not defined $self->align_filter) {
    $self->_set_align_filter($align_filter);
  }

  if (not defined $self->tag_index) {
    $self->_set_tag_index($tag_index);
  }

  return;
}

sub update_secondary_metadata {
  my ($self) = @_;

  # There are no secondary metadata for ancillary files

  $self->update_group_permissions;

  return $self;
}

after 'update_group_permissions' => sub {
  my ($self, $strict_groups) = @_;

  my $path  = $self->str;
  my $group = 'public';

  if ($strict_groups and none { $group eq $_ } $self->irods->list_groups) {
    $self->logconfess('Attempted to remove permissions for non-existent ',
                      "group '$group' on '$path'");
  }
  else {
    try {
      $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION, $group);
    } catch {
      if ($strict_groups) {
        $self->logconfess("Failed to remove permissions for group '$group' ",
                          "on '$path': ", $_);
      }
      else {
        $self->error("Failed to remove permissions for group '$group' ",
                     "on '$path': ", $_);
      }
    };
  }

  return $self;
};

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::AncFileDataObject

=head1 DESCRIPTION

Represents alignment/map (CRAM and BAM) files in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
