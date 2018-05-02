package WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use XML::LibXML;

use WTSI::NPG::HTS::PacBio::Metadata;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

# XML tags in the PacBio metadata XML files
our $RUN_TAG               = 'RunDetails';
our $NAME_TAG              = 'Name';
our $TS_NAME_TAG           = 'TimeStampedName';

our $SAMPLE_TAG            = 'WellSample';
our $WELL_TAG              = 'WellName';
our $SAMPLE_NAME_TAG       = 'Name';

our $COLLECTION_TAG        = 'CollectionMetadata';
our $INSTRUMENT_NAME_TAG   = 'InstrumentName';

our $COLL_NUMBER_TAG       = 'CollectionNumber';
our $CELL_INDEX_TAG        = 'CellIndex';

our $OUTPUT_TAG            = 'OutputOptions';
our $RFOLDER_TAG           = 'ResultsFolder';



=head2 parse_file

  Arg [1]    : Path to metadata xml file. Required.
  Arg [1]    : Prefix for extracting tags. Optional.

  Example    : my ($metadata) = $pub->parse_file($format);
  Description: Parse contents of XML file for key information.
  Returntype : WTSI::NPG::HTS::PacBio::Metadata

=cut

sub parse_file {
  my ($self, $file_path, $prefix) = @_;

  defined $file_path or
    $self->logconfess('A defined file_path argument is required');

  defined $prefix or $prefix = q[];

  my $dom = XML::LibXML->new->parse_file($file_path);

  my $run = $dom->getElementsByTagName($prefix . $RUN_TAG)->[0];
  my $run_name =
      $run->getElementsByTagName($prefix . $NAME_TAG)->[0]->string_value;
  my $ts_run_name =
      $run->getElementsByTagName($prefix . $TS_NAME_TAG)->[0]->string_value;

  my $sample = $dom->getElementsByTagName($prefix . $SAMPLE_TAG)->[0];
  my $well_name =
    $sample->getElementsByTagName($prefix . $WELL_TAG)->[0]->string_value;

  my $sample_name = $sample->getAttribute($SAMPLE_NAME_TAG);

  my $collection = $dom->getElementsByTagName($prefix . $COLLECTION_TAG)->[0];
  my $instrument_name = $collection->getAttribute($INSTRUMENT_NAME_TAG);

  my $collection_number =
      $dom->getElementsByTagName($prefix . $COLL_NUMBER_TAG)->[0]->string_value;
  my $cell_index =
      $dom->getElementsByTagName($prefix . $CELL_INDEX_TAG)->[0]->string_value;

  my $output =  $dom->getElementsByTagName($prefix . $OUTPUT_TAG)->[0];
  my $results_folder =
      $output->getElementsByTagName($prefix . $RFOLDER_TAG)->[0]->string_value;

  return WTSI::NPG::HTS::PacBio::Metadata->new
    (file_path          => $file_path,
     instrument_name    => $instrument_name,
     run_name           => $run_name,
     ts_run_name        => $ts_run_name,
     sample_name        => $sample_name,
     well_name          => $well_name,
     collection_number  => $collection_number,
     cell_index         => $cell_index,
     results_folder     => $results_folder,
     );
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser

=head1 DESCRIPTION

Parser for the Sequel PacBio metadata XML file(s) found in each SMRT 
cell subdirectory of completed run data.

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
