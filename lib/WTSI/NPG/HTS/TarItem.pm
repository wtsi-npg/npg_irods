package WTSI::NPG::HTS::TarItem;

use namespace::autoclean;

use Moose;
use MooseX::StrictConstructor;

our $VERSION= '';

has 'tar_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The path of the tar file containing the item');

has 'item_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The path (within the tar file) of the file item');

has 'checksum' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The checksum of the file item');

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::TarItem

=head1 DESCRIPTION

A TarManifest record describing a file in a tar archive in terms of
the path to the containing tar file, the relative path of the file
within the tar archive and the checksum of the tarred file.

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
