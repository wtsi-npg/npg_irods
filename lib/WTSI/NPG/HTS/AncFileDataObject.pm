package WTSI::NPG::HTS::AncFileDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[any];
use Moose;
use Try::Tiny;

our $VERSION = '';

our $PUBLIC = 'public'; # FIXME

# The contents of BED and JSON formatted file are sensitive and are
# given restricted access
our @RESTRICTED_ANCILLARY_FORMATS = qw[bed json];

extends 'WTSI::NPG::iRODS::DataObject';

with qw[
         WTSI::NPG::HTS::RunComponent
         WTSI::NPG::HTS::FilenameParser
         WTSI::NPG::HTS::AVUCollator
         WTSI::NPG::HTS::Annotator
       ];

has 'alignment_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   writer        => '_set_alignment_filter',
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

  my ($id_run, $position, $tag_index, $alignment_filter, $file_format) =
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

  if (not defined $self->alignment_filter) {
    $self->_set_alignment_filter($alignment_filter);
  }

  if (not defined $self->tag_index) {
    $self->_set_tag_index($tag_index);
  }

  return;
}

=head2 is_restricted_access

  Arg [1]      None

  Example    : $obj->is_restricted_access
  Description: Return true if the file contains or may contain sensitive
               information and is not for unrestricted public access.
  Returntype : Bool

=cut

sub is_restricted_access {
  my ($self) = @_;

  return any { $self->file_format eq $_ } @RESTRICTED_ANCILLARY_FORMATS;
}

sub update_secondary_metadata {
  my ($self, $factory, $with_spiked_control) = @_;

  defined $factory or
    $self->logconfess('A defined factory argument is required');

  my $path = $self->str;

  if ($self->is_restricted_access) {
    my $lims = $factory->make_lims($self->id_run, $self->position,
                                   $self->tag_index);

    # These files do not have full secondary metadata. They have
    # sufficient study metadata to set their permissions, if they are
    # restricted.
    my @meta = $self->make_study_metadata($lims, $with_spiked_control);
    $self->debug("Created metadata AVUs for '$path' : ", pp(\@meta));

    # Collate into lists of values per attribute
    my %collated_avus = %{$self->collate_avus(@meta)};

    # Sorting by attribute to allow repeated updates to be in
    # deterministic order
    my @attributes = sort keys %collated_avus;
    $self->debug("Superseding AVUs on '$path' in order of attributes: ",
                 join q[, ], @attributes);
    foreach my $attr (@attributes) {
      my $values = $collated_avus{$attr};
      try {
        $self->supersede_multivalue_avus($attr, $values, undef);
      } catch {
        $self->error("Failed to supersede with attribute '$attr' and values ",
                     pp($values), q[: ], $_);
      };
    }

    $self->info("Setting study-defined access for restricted file '$path'");
    $self->update_group_permissions;
  }
  else {
    $self->update_group_permissions;
    $self->info("Setting public access for unrestricted file '$path'");
    $self->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION, 'public');
  }

  return $self;
}

before 'update_group_permissions' => sub {
  my ($self) = @_;

  # If the data contains any non-consented human data, or we are
  # expecting to set groups restricting general access, then remove
  # access for the public group.
  if ($self->is_restricted_access) {
    $self->info(qq[Removing $PUBLIC access to '], $self->str, q[']);
    $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION, $PUBLIC);
  }
  else {
    $self->info(qq[Allowing $PUBLIC access to '], $self->str, q[']);
  }
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
