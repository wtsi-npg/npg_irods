package WTSI::NPG::HTS::PacBio::AnalysisPublisherBase;

use File::Spec::Functions qw[catdir];
use Moose::Role;
use MooseX::StrictConstructor;
use WTSI::NPG::HTS::PacBio::Metadata;
use WTSI::NPG::HTS::PacBio::MetaXMLParser;

our $VERSION = '';

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

# Metadata related
our $METADATA_FORMAT = 'xml';
our $METADATA_PREFIX = 'pbmeta:';
our $METADATA_SET    = q{consensusreadset};
our $SMT_METADATA_SET = q{hifi_reads.consensusreadset};

# Location of source metadata file
our $OUTPUT_DIR      = 'outputs';

has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');

has 'is_oninstrument' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Set if the analysis was done on the instrument or in SMRT Link where publishable files are in analysis sub-directories. Historically if analysis is done in SMRT Link then all standard publishable files will be found in the analysis directory whereas if the analysis is done on the instrument or in a post v11.0 version of SMRT Link publishable deplexed bam, index and xml files are to be found in one or more sub-directories of the specified analysis path.');

has 'is_smtwelve' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Set to true if SMRT Link v12+ oninstrument files.',);

has '_metadata' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   builder       => '_build_metadata',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Load source meta data from file.',);

sub _build_metadata{
  my ($self) = @_;

  if ( !defined $self->_metadata_file ) {
    $self->logcroak('Metadata files is not defined for '. $self->analysis_path);
  }

  return  WTSI::NPG::HTS::PacBio::MetaXMLParser->new->parse_file
                 ($self->_metadata_file, $METADATA_PREFIX);
}

has '_metadata_file' =>
  (isa           => 'Str',
   is            => 'ro',
   builder       => '_build_metadata_file',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Source meta data file.',);

sub _build_metadata_file{
  my ($self) = @_;

  my $output_path = catdir($self->analysis_path, $OUTPUT_DIR);

  my @metafiles;
  if (-d $output_path) {
    # As all analysis cell based all metafiles should have the correct run name,
    # well and plate number as no merged cell analysis - so just pick one.
    my @files = $self->list_directory
      ($output_path, filter => $METADATA_SET .q[.]. $METADATA_FORMAT . q[$]);
    push @metafiles, $files[0];
  } elsif ($self->is_oninstrument == 1 && $self->is_smtwelve == 1) {
    # Revio
    @metafiles = $self->list_directory
      ($self->analysis_path,
       filter => $self->movie_pattern .q[.]. $SMT_METADATA_SET .q[.]. $METADATA_FORMAT .q[$],
       recurse => 1)
  } elsif ($self->is_oninstrument == 1 ) {
    # Sequel IIe - as will never be upgraded from ICS v11
    @metafiles = $self->list_directory
      ($self->analysis_path,
       filter => $self->movie_pattern .q[.]. $METADATA_SET .q[.]. $METADATA_FORMAT .q[$])
  }

  if (@metafiles != 1) {
    $self->logcroak('Expect one xml file in '. $self->analysis_path);
  }
  return $metafiles[0];
}

sub run_name {
  my ($self) = @_;
  return $self->_metadata->ts_run_name;
};

sub smrt_names {
  my ($self)  = @_;

  ($self->_metadata->has_results_folder &&
      $self->_metadata->ts_run_name) or
      $self->logconfess('Error ts or results folder missing');

  my $rfolder = $self->_metadata->results_folder;
  my $ts_name = $self->_metadata->ts_run_name;

  $rfolder =~ /$ts_name/smx or
     $self->logconfess('Error ts name missing from results folder ', $rfolder);

  $rfolder =~ s/$ts_name//smx;
  $rfolder =~ s/\///gsmx;

  $rfolder =~ /$WELL_DIRECTORY_PATTERN/smx or
     $self->logconfess('Error derived folder name ', $rfolder,
     'does not match expected pattern');

  return [$rfolder];
};


no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::AnalysisPublisherBase

=head1 DESCRIPTION

Attributes and methods used by PacBio Analyis publisher modules

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2024 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
