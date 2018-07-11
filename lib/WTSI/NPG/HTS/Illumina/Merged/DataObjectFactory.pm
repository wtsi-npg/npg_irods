package WTSI::NPG::HTS::Illumina::Merged::DataObjectFactory;

use namespace::autoclean;
use File::Basename;
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::HTS::Illumina::Merged::AlnDataObject;

extends qw[WTSI::NPG::HTS::Illumina::DataObjectFactory];

our $VERSION = '';

my $align_regex   = qr{[.](cram)$}msx;

=head2 make_data_object

  Arg [1]      Data object path, Str.

  Example    : my $obj = $factory->make_data_object($path);
  Description: Return a new data object for a path. If the factory cannot
               construct a suitable object for the given path, it may
               return undef.
  Returntype : WTSI::NPG::HTS::DataObject or undef

=cut

override 'make_data_object' => sub {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');
  $path or $self->logconfess('A non-empty path argument is required');

  my $obj;

  my ($filename, $collection, $suffix) = fileparse($path);
  my @init_args = (collection  => $collection,
                   data_object => $filename,
                   irods       => $self->irods);

  if ($filename =~ m{$align_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::Merged::AlnDataObject from ',
                 "'$path' matching $align_regex");
    $obj = WTSI::NPG::HTS::Illumina::Merged::AlnDataObject->new(@init_args);
  }
  else {
    $self->debug("Not making any WTSI::NPG::HTS::DataObject for '$path'");
  }

  return $obj;
};

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::Merged::DataObjectFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files from an
Illumina sequencing run. Different types of local file may require
data objects of different classes and an object of this class will
construct the appropriate one.

=head1 AUTHOR

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
