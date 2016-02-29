package WTSI::NPG::HTS::XMLFileDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[any];
use Moose;
use Try::Tiny;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::DataObject';

with qw[
         npg_tracking::glossary::run
       ];

=head2 is_restricted_access

  Arg [1]      None

  Example    : $obj->is_restricted_access
  Description: Return true if the file contains or may contain sensitive
               information and is not for unrestricted public access.
  Returntype : Bool

=cut

sub is_restricted_access {
  my ($self) = @_;

  return 0;
}

sub update_secondary_metadata {
  my ($self) = @_;

  $self->update_group_permissions;

  return $self;
}

before 'update_group_permissions' => sub {
  my ($self) = @_;

  # If the data contains any non-consented human data, or we are
  # expecting to set groups restricting general access, then remove
  # access for the public group.
  if ($self->is_restricted_access) {
    $self->info(qq[Removing $WTSI::NPG::iRODS::PUBLIC_GROUP access to '],
                $self->str, q[']);
    $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                           $WTSI::NPG::iRODS::PUBLIC_GROUP);
  }
  else {
    $self->info(qq[Allowing $WTSI::NPG::iRODS::PUBLIC_GROUP access to '],
                $self->str, q[']);
  }
};

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::XMLFileDataObject

=head1 DESCRIPTION

Represents XML (RunInfo.xml and runparameters.xml) files in iRODS.

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
