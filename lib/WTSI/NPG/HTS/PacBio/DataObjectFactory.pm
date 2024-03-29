package WTSI::NPG::HTS::PacBio::DataObjectFactory;

use namespace::autoclean;
use File::Basename;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::HTS::PacBio::SeqDataObject;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::DataObjectFactory
       ];

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   documentation => 'The iRODS connection handle');

my $sequence_regex = qr{[.](h5|bam|fasta.gz)$}msx;


=head2 make_data_object

  Arg [1]      Data object path, Str.

  Example    : my $obj = $factory->make_data_object($path);
  Description: Return a new data object for a path.
  Returntype : WTSI::NPG::HTS::DataObject

=cut

sub make_data_object {
  my ($self, $remote_path) = @_;

  defined $remote_path or
    $self->logconfess('A defined remote_path argument is required');
  length $remote_path or
    $self->logconfess('A non-empty remote_path argument is required');

  my ($objname, $collection, $ignore) = fileparse($remote_path);

  my $obj;
  if ($objname =~ m{$sequence_regex}mxsi) {
    $obj =
      WTSI::NPG::HTS::PacBio::SeqDataObject->new(collection  => $collection,
                                                 data_object => $objname,
                                                 irods       => $self->irods);
  }
  else {
    $obj = WTSI::NPG::HTS::DataObject->new(collection  => $collection,
                                           data_object => $objname,
                                           irods       => $self->irods);
  }

  return $obj;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::DataObjectFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files from an
PacBio sequencing run. Different types of local file may require
data objects of different classes and an object of this class will
construct the appropriate one.

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
