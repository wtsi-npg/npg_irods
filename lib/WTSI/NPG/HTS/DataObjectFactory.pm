package WTSI::NPG::HTS::DataObjectFactory;

use Moose::Role;

our $VERSION = '';

requires 'make_data_object';

=head2 make_data_object

  Arg [1]      Data object path, Str.

  Example    : my $obj = $factory->make_data_object($path);
  Description: Return a new data object for a path. If the factory cannot
               construct a suitable object for the given path, it may
               return undef.
  Returntype : WTSI::NPG::HTS::DataObject or undef

=cut

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::DataObjectFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files. The
factory will determine what class(es) of data object to construct.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
