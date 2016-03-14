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

my $anc_pattern = join q[|], qw[bai bed bamcheck crai flagstat json
                                seqchksum stats txt];

my $almap_regex = qr{[.](bam|cram)$}msx;
my $anc_regex   = qr{[.]($anc_pattern)$}msx;
my $xml_regex   = qr{[.]xml$}msx;

{
  my $positional = 2;
  my @named      = qw[id_run position tag_index];
  my $params = function_params($positional, @named);

=head2 make_data_object

  Arg [1]      Data object path, Str.

  Example    : my $obj = $factory->make_data_object($path);
  Description: Return a new data object for a path. If the factory cannot
               construct a suitable object for the given path, it may
               return undef.
  Returntype : WTSI::NPG::HTS::DataObject or undef

=cut

  sub make_data_object {
    my ($self, $path) = $params->parse(@_);

    defined $path or $self->logconfess('A defined path argument is required');
    $path or $self->logconfess('A non-empty path argument is required');

    my $obj;

    my ($filename, $collection, $suffix) = fileparse($path);

    my @init_args = (collection  => $collection,
                     data_object => $filename,
                     irods       => $self->irods,
                     logger      => $self->logger);

    if ($filename =~  m{$almap_regex}msxi) {
      $self->debug("Making WTSI::NPG::HTS::AlMapFileDataObject from '$path' ",
                   "matching $almap_regex");
      $obj = WTSI::NPG::HTS::AlMapFileDataObject->new(@init_args);
    }
    elsif ($filename =~ m{$anc_regex}msxi) {
      $self->debug("Making WTSI::NPG::HTS::AncFileDataObject from '$path' ",
                   "matching $anc_regex");
      $obj = WTSI::NPG::HTS::AncFileDataObject->new(@init_args);
    }
    elsif ($filename =~ m{$xml_regex}msxi) {
      if (defined $params->id_run) {
        push @init_args, id_run => $params->id_run;
      }
      $self->debug("Making WTSI::NPG::HTS::XMLFileDataObject from '$path' ",
                   "matching $xml_regex");
      $obj = WTSI::NPG::HTS::XMLFileDataObject->new(@init_args);
    }
    else {
      $self->debug("Not making any WTSI::NPG::HTS::DataObject for '$path'");
      # return undef
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
