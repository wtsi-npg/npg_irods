package WTSI::NPG::HTS::PacBio::SeqDataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use WTSI::NPG::iRODS::Metadata qw[$PACBIO_SOURCE];

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';


has '+is_restricted_access' =>
  (is            => 'ro');


sub _build_is_restricted_access {
  my ($self) = @_;

  my $is_restricted = 0;
  if($self->find_in_metadata($PACBIO_SOURCE)){
      $is_restricted = 1;
  }

  return $is_restricted;
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::SeqDataObject

=head1 DESCRIPTION

Represents an PacBio sequence file in iRODS. This class overrides 
some base class behaviour to introduce handling of the 'public'
group during 'update_group_permissions' calls.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights
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
