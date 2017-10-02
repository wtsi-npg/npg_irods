package WTSI::NPG::HTS::ONT::TarDataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::HTS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';

has '+is_restricted_access' =>
  (is            => 'ro');

has '+primary_metadata' =>
  (is            => 'ro');

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::TarDataObject

=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
