package WTSI::NPG::HTS::IlluminaObjFactory;

use namespace::autoclean;
use File::Basename;
use Moose;
use MooseX::StrictConstructor;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::AlMapFileDataObject;
use WTSI::NPG::HTS::AncFileDataObject;
use WTSI::NPG::HTS::XMLFileDataObject;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::DataObjectFactory
       ];

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   default       => sub { return WTSI::NPG::iRODS->new },
   documentation => 'The iRODS connection handle');

{
  my $positional = 3; ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
  my @named      = qw[id_run position tag_index];
  my $params = function_params($positional, @named);

  sub make_data_object {
    my ($self, $dest_collection, $file) = $params->parse(@_);

    defined $dest_collection or
      $self->logconfess('A defined dest_collection argument is required');
    $dest_collection or
      $self->logconfess('A non-empty dest_collection argument is required');
    defined $file or
      $self->logconfess('A defined file argument is required');
    $file or $self->logconfess('A non-empty file argument is required');

    my $obj;

    my $filename = fileparse($file);
    if ($filename =~  m{[.](bam|cram)$}msxi) {
      $obj = WTSI::NPG::HTS::AlMapFileDataObject->new
        (collection  => $dest_collection,
         data_object => $filename,
         irods       => $self->irods,
         logger      => $self->logger);
    }
    elsif ($filename =~  m{[.]xml$}msxi) {
      $obj = WTSI::NPG::HTS::XMLFileDataObject->new
        (collection  => $dest_collection,
         data_object => $filename,
         id_run      => $params->id_run,
         irods       => $self->irods);
    }
    else {
      $obj = WTSI::NPG::HTS::AncFileDataObject->new
        (collection  => $dest_collection,
         data_object => $filename,
         irods       => $self->irods);
    }

    return $obj;
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::IlluminaObjFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files from an
Illumina sequencing run.

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
