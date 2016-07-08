package WTSI::NPG::HTS::PacBio::Metadata;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

has 'file_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The path of the metadata XML file');

has 'run_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML run name');

has 'sample_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML sample');

has 'well_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML well name');

has 'instrument_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML instrument name');

has 'collection_number' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML collection number');

has 'cell_index' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML cell index');

has 'set_number' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio XML set number');

has 'library_tube_uuids' =>
  (isa           => 'ArrayRef[Str]',
   is            => 'ro',
   required      => 1,
   documentation => 'The WTSI LIMS library tube UUIDs');

has 'run_uuid' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The WTSI LIMS PacBio run UUID');

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Metadata

=head1 DESCRIPTION

Represents excerpts of the PacBio metadata created per SMRT cell.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

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
