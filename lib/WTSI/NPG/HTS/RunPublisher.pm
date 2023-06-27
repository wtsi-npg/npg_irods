package WTSI::NPG::HTS::RunPublisher;

use namespace::autoclean;
use Moose::Role;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
       ];

our $VERSION = '';

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'source_directory' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The directory in which to find data to publish');

has 'mlwh_locations' =>
  (isa           => 'WTSI::NPG::HTS::LocationWriter',
   is            => 'ro',
   required      => 0,
   documentation => 'An object used to build and write information to be ' .
                    'loaded into the seq_product_irods_locations table.');

sub _build_dest_collection  {
  my ($self) = @_;

  return;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::RunPublisher

=head1 DESCRIPTION

A role to be consumed by data publishers that read files from a local
source directory and write them to a remote iRODS destination
collection.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018, 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
