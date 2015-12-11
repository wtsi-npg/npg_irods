package WTSI::NPG::HTS::Annotator;

use Data::Dump qw(pp);
use DateTime;
use Encode; # FIXME
use File::Basename;
use Moose::Role;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

our @GENERAL_PURPOSE_SUFFIXES = qw(.csv .tif .tsv  .txt .xls .xlsx .xml);
our @GENO_DATA_SUFFIXES       = qw(.gtc .idat);
our @HTS_DATA_SUFFIXES        = qw(.bam .cram);
our @HTS_ANCILLARY_SUFFIXES   = qw(.bamcheck .bed .flagstat .seqchksum
                                   .stats .xml);

our @DEFAULT_FILE_SUFFIXES = (@GENERAL_PURPOSE_SUFFIXES,
                              @GENO_DATA_SUFFIXES,
                              @HTS_DATA_SUFFIXES,
                              @HTS_ANCILLARY_SUFFIXES);

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::iRODS::Utilities';

# See http://dublincore.org/documents/dcmi-terms/

=head2 make_creation_metadata

  Arg [1]    : Creating person, organization, or service, URI.
  Arg [2]    : Creation time, DateTime
  Arg [3]    : Publishing person, organization, or service, URI.

  Example    : my @meta = $ann->make_creation_metadata($time, $publisher)
  Description: Return a list of metadata AVUs describing the creation of
               an item.
  Returntype : Array[HashRef]

=cut

sub make_creation_metadata {
  my ($self, $creator, $creation_time, $publisher) = @_;

  defined $creation_time or
    $self->logconfess('A defined creator argument is required');
  defined $creation_time or
    $self->logconfess('A defined creation_time argument is required');
  defined $publisher or
    $self->logconfess('A defined publisher argument is required');

  return ($self->make_avu($DCTERMS_CREATOR,   $creator->as_string),
          $self->make_avu($DCTERMS_CREATED,   $creation_time->iso8601),
          $self->make_avu($DCTERMS_PUBLISHER, $publisher->as_string));
}

=head2 make_modification_metadata

  Arg [1]    : Modification time, DateTime.

  Example    : my @meta = $ann->make_modification_metadata($time)
  Description: Return an array of of metadata AVUs describing the
               modification of an item.
  Returntype : Array[HashRef]

=cut

sub make_modification_metadata {
  my ($self, $modification_time) = @_;

  defined $modification_time or
    $self->logconfess('A defined modification_time argument is required');

  return ($self->make_avu($DCTERMS_MODIFIED, $modification_time->iso8601));
}

=head2 make_type_metadata

  Arg [1]    : File name, Str.
  Arg [2]    : Array of valid file suffix strings, Str. Optional

  Example    : my @meta = $ann->make_type_metadata($sample, '.txt', '.csv')
  Description: Return an array of metadata AVUs describing the file 'type'
               (represented by its suffix).
  Returntype : Array[HashRef]

=cut

sub make_type_metadata {
  my ($self, $file, @suffixes) = @_;

  defined $file or $self->logconfess('A defined file argument is required');
  $file eq q{} and $self->logconfess('A non-empty file argument is required');

  if (not @suffixes) {
    @suffixes = @DEFAULT_FILE_SUFFIXES;
  }

  my ($basename, $dir, $suffix) = fileparse($file, @suffixes);

  my @meta;
  if ($suffix) {
    my ($base_suffix) = $suffix =~ m{^[.]?(.*)}msx;
    push @meta, $self->make_avu($FILE_TYPE, $base_suffix);
  }

  return @meta;
}

=head2 make_md5_metadata

  Arg [1]    : Checksum, Str.

  Example    : my @meta = $ann->make_md5_metadata($checksum)
  Description: Return an array of metadata AVUs describing the
               file MD5 checksum.
  Returntype : Array[HashRef]

=cut

sub make_md5_metadata {
  my ($self, $md5) = @_;

  defined $md5 or $self->logconfess('A defined md5 argument is required');
  $md5 eq q{} and $self->logconfess('A non-empty md5 argument is required');

  return ($self->make_avu($FILE_MD5, $md5));
}

=head2 make_ticket_metadata

  Arg [1]    : string filename

  Example    : my @meta = $ann->make_ticket_metadata($ticket_number)
  Description: Return an array of metadata AVUs describing an RT ticket
               relating to the file.
  Returntype : Array[HashRef]

=cut

sub make_ticket_metadata {
  my ($self, $ticket_number) = @_;

  defined $ticket_number or
    $self->logconfess('A defined ticket_number argument is required');
  $ticket_number eq q{} and
    $self->logconfess('A non-empty ticket_number argument is required');

  return ($self->make_avu($RT_TICKET, $ticket_number));
}

=head2 make_hts_metadata

  Arg [1]    : Factory for st:api::lims objects, WTSI::NPG::HTS::LIMSFactory.
  Arg [2]    : Run identifier, Int.
  Arg [3]    : Flowcell lane position, Int.
  Arg [4]    : Tag index, Int. Optional.

  Example    : my @meta = $ann->make_hts_metadata($factory, 3002, 3, 1)
  Description: Return an array of metadata AVUs describing the HTS data
               in the specified run/lane/plex.
  Returntype : Array[HashRef]

=cut

## no critic (Subroutines::ProhibitManyArgs)
sub make_hts_metadata {
  my ($self, $factory, $id_run, $position, $tag_index,
      $with_spiked_control) = @_;

  defined $factory or
    $self->logconfess('A defined factory argument is required');
  defined $id_run or
    $self->logconfess('A defined id_run argument is required');
  defined $position or
    $self->logconfess('A defined position argument is required');

  my $lims = $factory->make_lims($id_run, $position, $tag_index);

  my @meta;
  push @meta, $self->make_plex_metadata($lims);
  push @meta, $self->make_consent_metadata($lims);
  push @meta, $self->make_study_metadata($lims, $with_spiked_control);
  push @meta, $self->make_sample_metadata($lims, $with_spiked_control);
  push @meta, $self->make_library_metadata($lims, $with_spiked_control);

  my $hts_element = sprintf 'run: %s, pos: %s, tag_index: %s',
    $id_run, $position, (defined $tag_index ? $tag_index : 'NA');
  $self->info("Created metadata for $hts_element: ", pp(\@meta));

  return @meta;
}
## use critic

=head2 make_run_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_run_metadata($st);
  Description: Return HTS run metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_run_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {id_run    => $ID_RUN,
                     position  => $POSITION,
                     tag_index => $TAG_INDEX};
  return $self->_make_single_value_metadata($lims, $method_attr);
}

=head2 make_study_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_study_metadata($st);
  Description: Return HTS study metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_metadata {
  my ($self, $lims, $with_spiked_control) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr =
    {study_accession_numbers => $STUDY_ACCESSION_NUMBER,
     study_names             => $STUDY_NAME,
     study_ids               => $STUDY_ID,
     study_titles            => $STUDY_TITLE};

  return $self->_make_multi_value_metadata($lims, $method_attr,
                                           $with_spiked_control);
}

=head2 make_sample_metadata

  Arg [1]    : A LIMS handle, st::api::lims.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my @meta = $ann->make_sample_metadata($lims);
  Description: Return HTS sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, $lims, $with_spiked_control) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr =
    {sample_names          => $SAMPLE_NAME,
     sample_public_names   => $SAMPLE_PUBLIC_NAME,
     sample_common_names   => $SAMPLE_COMMON_NAME,
     sample_supplier_names => $SAMPLE_SUPPLIER_NAME,
     sample_cohorts        => $SAMPLE_COHORT,
     sample_donor_ids      => $SAMPLE_DONOR_ID};

  return $self->_make_multi_value_metadata($lims, $method_attr,
                                           $with_spiked_control);
}

=head2 make_consent_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_consent_metadata($lims);
  Description: Return HTS consent metadata AVUs. An AVU will be returned
               only if a true AVU value is present.
  Returntype : Array[HashRef]

=cut

sub make_consent_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  my $attr  = $SAMPLE_CONSENT_WITHDRAWN;
  my $value = $lims->any_sample_consent_withdrawn;

  my @avus;
  if ($value) {
    push @avus, $self->make_avu($attr, $value);
  }

  return @avus;
}

=head2 make_library_metadata

  Arg [1]    : A LIMS handle, st::api::lims.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my @meta = $ann->make_library_metadata($lims);
  Description: Return HTS library metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_library_metadata {
  my ($self, $lims, $with_spiked_control) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {library_ids => $LIBRARY_ID};

  return $self->_make_multi_value_metadata($lims, $method_attr,
                                           $with_spiked_control);
}

=head2 make_plex_metadata

  Arg [1]    :  A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_plex_metadata($lims);
  Description: Return HTS plex metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_plex_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {qc_state => $QC_STATE};
  return $self->_make_single_value_metadata($lims, $method_attr);
}

sub _make_single_value_metadata {
  my ($self, $lims, $method_attr) = @_;
  # The method_attr argument is a map of method name to attribute name
  # under which the result will be stored.

  my @avus;
  foreach my $method_name (sort keys %{$method_attr}) {
    my $attr  = $method_attr->{$method_name};
    my $value = $lims->$method_name;

    if (defined $value) {
      $self->debug("st::api::lims::$method_name returned ", $value);

      $attr  = decode('UTF-8', $attr,  Encode::FB_CROAK); # FIXME
      $value = decode('UTF-8', $value, Encode::FB_CROAK); # FIXME

      push @avus, $self->make_avu($attr, $value);
    }
    else {
      $self->debug("st::api::lims::$method_name returned undef");
    }
  }

  return @avus;
}

sub _make_multi_value_metadata {
  my ($self, $lims, $method_attr, $with_spiked_control) = @_;
  # The method_attr argument is a map of method name to attribute name
  # under which the result will be stored.

  my @avus;
  foreach my $method_name (sort keys %{$method_attr}) {
    my $attr = $method_attr->{$method_name};

    $attr  = decode('UTF-8', $attr,  Encode::FB_CROAK); # FIXME

    my @values = $lims->$method_name($with_spiked_control);
    $self->debug("st::api::lims::$method_name returned ", pp(\@values));

    foreach my $value (@values) {
      $value = decode('UTF-8', $value, Encode::FB_CROAK); # FIXME

      push @avus, $self->make_avu($attr, $value);
    }
  }

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for WTSI HTS runs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
