package WTSI::NPG::HTS::PacBio::MetaXMLParser;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use XML::LibXML;

use WTSI::NPG::HTS::PacBio::Metadata;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

# XML tags in the PacBio metadata XML files
our $CUSTOM_TAG            = 'Custom';
our $NAME_TAG              = 'Name';
our $RUN_TAG               = 'Run';
our $SAMPLE_TAG            = 'Sample';

our $CELL_INDEX_TAG        = 'CellIndex';
our $COLLECTION_NUMBER_TAG = 'CollectionNumber';
our $INSTRUMENT_NAME_TAG   = 'InstrumentName';
our $SET_NUMBER_TAG        = 'SetNumber';
our $WELL_TAG              = 'WellName';

# XML attributes
our $LABEL_ATTR = 'label';
our $USER_DEFINED_FIELD_3 = 'User Defined Field 3';

sub parse_file {
  my ($self, $file_path) = @_;

  defined $file_path or
    $self->logconfess('A defined file_path argument is required');

  my $dom = XML::LibXML->new->parse_file($file_path);

  my $run = $dom->getElementsByTagName($RUN_TAG)->[0];
  my $run_name =
    $run->getElementsByTagName($NAME_TAG)->[0]->string_value;

  my $sample = $dom->getElementsByTagName($SAMPLE_TAG)->[0];
  my $sample_name =
    $sample->getElementsByTagName($NAME_TAG)->[0]->string_value;
  my $well_name =
    $sample->getElementsByTagName($WELL_TAG)->[0]->string_value;

  my $instrument_name =
    $dom->getElementsByTagName($INSTRUMENT_NAME_TAG)->[0]->string_value;
  my $collection_number =
    $dom->getElementsByTagName($COLLECTION_NUMBER_TAG)->[0]->string_value;
  my $cell_index =
    $dom->getElementsByTagName($CELL_INDEX_TAG)->[0]->string_value;
  my $set_number =
    $dom->getElementsByTagName($SET_NUMBER_TAG)->[0]->string_value;

  my $run_uuid;

  my @props = $dom->getElementsByTagName($CUSTOM_TAG)->[0]->findnodes('./*');
  foreach my $property (@props) {
    if ($property->textContent =~ /\w/mxs) {
      my $attr = $property->getAttribute($LABEL_ATTR);
      # May have multiple packed values :(
      my @values = split /;/msx, $property->textContent;

      if ($attr eq $USER_DEFINED_FIELD_3) {
        $run_uuid = shift @values;      # maps to iRODS batch_id attribute
      }
    }
  }

  return WTSI::NPG::HTS::PacBio::Metadata->new
    (cell_index         => $cell_index,
     collection_number  => $collection_number,
     file_path          => $file_path,
     instrument_name    => $instrument_name,
     run_name           => $run_name,
     run_uuid           => $run_uuid,
     sample_name        => $sample_name,
     set_number         => $set_number,
     well_name          => $well_name);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::MetaXMLParser

=head1 DESCRIPTION

Parser for the PacBio metadata XML file(s) found in each SMRT cell
subdirectory of completed run data.

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
