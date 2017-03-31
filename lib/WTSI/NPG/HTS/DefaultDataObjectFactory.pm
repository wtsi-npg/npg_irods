package WTSI::NPG::HTS::DefaultDataObjectFactory;

use namespace::autoclean;
use File::Basename;
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::DataObjectFactory
       ];

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   lazy          => 1,
   builder       => '_build_irods',
   documentation => 'The iRODS connection handle');

sub make_data_object {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');
  $path or $self->logconfess('A non-empty path argument is required');

  my ($filename, $collection, $suffix) = fileparse($path);

  return WTSI::NPG::HTS::DataObject->new(collection  => $collection,
                                         data_object => $filename,
                                         irods       => $self->irods);
}

sub _build_irods {
  return WTSI::NPG::iRODS->new;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::HTS::DefaultDataObjectFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files. This default
implementation creates WTSI::NPG::HTS::DataObject instances.

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
