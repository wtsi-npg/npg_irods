package WTSI::NPG::HTS::DataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use File::Basename;
use List::AllUtils qw[any];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::DataObject';

with 'WTSI::NPG::HTS::AVUCollator';

has 'file_format' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 1,
   builder       => '_build_file_format',
   lazy          => 1,
   documentation => 'The storage format of the file');

has 'is_restricted_access' =>
  (is            => 'rw',
   isa           => 'Bool',
   required      => 1,
   builder       => '_build_is_restricted_access',
   lazy          => 1,
   documentation => 'If true, the data object will not have read access '.
                    'for iRODS data access group "public"');

has 'primary_metadata' =>
  (is            => 'rw',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [] },
   documentation => 'The primary metadata (AVU) attributes recognised by ' .
                    'this type of data object. The default is an empty array ' .
                    'which signifies no restriction on attributes.');

has 'secondary_metadata' =>
  (is            => 'rw',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [] },
   documentation => 'The secondary metadata (AVU) attributes recognised by ' .
                    'this type of data object. The default is an empty array ' .
                    'which signifies no restriction on attributes.');

=head2 is_restricted_access

  Arg [1]      None

  Example    : $obj->is_restricted_access
  Description: Return true if the file contains or may contain sensitive
               information and is not for unrestricted public access.
               The is true for all alignment files. If public access is
               required, members of the "public" group are added explicitly
               to the relevant study data access group.
  Returntype : Bool

=cut

=head2 is_primary_metadata

  Arg [1]    : AVU, HashRef. A candidate iRODS AVU.

  Example    : $obj->is_primary_metadata({attribute => 'y', value => 'y'});
  Description: Return true if the candidate AVU argument is acceptable as
               primary metadata for the data object.
  Returntype : Bool

=cut

sub is_primary_metadata {
  my ($self, $avu) = @_;

  return $self->_is_valid_metadata($avu, $self->primary_metadata);
}

=head2 is_secondary_metadata

  Arg [1]    : AVU, HashRef. A candidate iRODS AVU.

  Example    : $obj->is_secondary_metadata({attribute => 'y', value => 'y'});
  Description: Return true if the candidate AVU argument is acceptable as
               secondary metadata for the data object.
  Returntype : Bool

=cut

sub is_secondary_metadata {
  my ($self, $avu) = @_;

  return $self->_is_valid_metadata($avu, $self->secondary_metadata);
}

=head2 set_primary_metadata

  Arg [1]    : AVUs to add, List[HashRef]. A list of candidate iRODS AVUs.

  Example    : $obj->set_primary_metadata({attribute => 'y', value => 'y'});
  Description: Test each candidate AVU against the 'is_primary_metadata'
               predicate and if a true value is returned, set it as
               metadata on the object. Return $self;
  Returntype : WTSI::NPG::HTS::DataObject

=cut

sub set_primary_metadata {
  my ($self, @avus) = @_;

  my @primary_avus = grep { $self->is_primary_metadata($_) } @avus;
  return $self->_set_metadata(@primary_avus);
}

=head2 update_secondary_metadata

  Arg [1]    : AVUs to add, List[HashRef]. A list of iRODS AVUs.

  Example    : $obj->update_secondary_metadata(@avus);
  Description: Update all secondary (LIMS-supplied) metadata using the
               supplied AVUs. Unlike primary metadata, the object does
               not filter the AVUs, so you are free to add your own.
               Return $self.
  Returntype : WTSI::NPG::HTS::DataObject

=cut

sub update_secondary_metadata {
  my ($self, @avus) = @_;

  my @secondary_avus = grep { $self->is_secondary_metadata($_) } @avus;
  return $self->_set_metadata(@secondary_avus);
}

after 'update_secondary_metadata' => sub {
  my ($self) = @_;

  $self->update_group_permissions;
};

before 'update_group_permissions' => sub {
  my ($self) = @_;

  my $path = $self->str;
  if ($self->is_restricted_access) {
    $self->info("Removing $WTSI::NPG::iRODS::PUBLIC_GROUP access to '$path'");
    $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                           $WTSI::NPG::iRODS::PUBLIC_GROUP);
  }
  else {
    $self->info("Allowing $WTSI::NPG::iRODS::PUBLIC_GROUP access to '$path'");
  }
};

after 'update_group_permissions' => sub {
  my ($self) = @_;

  if ($self->is_restricted_access) {
    # Do nothing for restricted files
  }
  else {
    my $path = $self->str;
    $self->info("Setting public access for unrestricted file '$path'");
    $self->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                           $WTSI::NPG::iRODS::PUBLIC_GROUP);
  }
};

sub _build_file_format {
  my ($self) = @_;

  my $path = $self->str;
  my ($file_format) = $path =~ m{[.]([^.]+)$}msx;

  if ($file_format) {
    $self->debug("Parsed file format suffix '$file_format' from '$path'");
  }
  else {
    $self->error("Failed to parse a file format suffix from '$path'");
  }

  return $file_format;
}

# Override this in subclass for restricted access
sub _build_is_restricted_access {
  my ($self) = @_;

  return 0;
}

sub _is_valid_metadata {
  my ($self, $avu, $reference_avus) = @_;

  defined $avu or $self->logconfess('A defined avu argument is required');
  ref $avu eq 'HASH' or
    $self->logconfess('The avu argument must be a HashRef');

  my $attr = $avu->{attribute};
  return (defined $attr and
          (scalar @{$reference_avus} == 0 or
           any { $attr eq $_ } @{$reference_avus}));
}

sub _set_metadata {
  my ($self, @avus) = @_;

  my $path = $self->str;
  # Collate into lists of values per attribute
  my %collated_avus = %{$self->collate_avus(@avus)};

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

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::DataObject

=head1 DESCRIPTION

The base class for all HTS data objects, defining default metadata
behaviour.

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
