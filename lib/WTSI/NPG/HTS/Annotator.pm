package WTSI::NPG::HTS::Annotator;

use Data::Dump qw[pp];
use DateTime;
use Moose::Role;

use WTSI::NPG::iRODS::Metadata;
use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

our @GENERAL_PURPOSE_SUFFIXES = qw[csv tif tsv txt xls xlsx xml];
our @GENO_DATA_SUFFIXES       = qw[gtc idat];
our @HTS_DATA_SUFFIXES        = qw[bam cram bai crai];
our @HTS_ANCILLARY_SUFFIXES   = qw[bamcheck bed flagstat json seqchksum
                                   stats xml];

our @DEFAULT_FILE_SUFFIXES = (@GENERAL_PURPOSE_SUFFIXES,
                              @GENO_DATA_SUFFIXES,
                              @HTS_DATA_SUFFIXES,
                              @HTS_ANCILLARY_SUFFIXES);
our $SUFFIX_PATTERN = join q[|], @DEFAULT_FILE_SUFFIXES;
our $SUFFIX_REGEX   = qr{[.]($SUFFIX_PATTERN)$}msx;

# Sequence alignment filters
our $YHUMAN = 'yhuman';  # FIXME

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::iRODS::Utilities
       ];

# See http://dublincore.org/documents/dcmi-terms/

=head2 make_creation_metadata

  Arg [1]    : Creating person, organization, or service, URI.
  Arg [2]    : Creation time, DateTime
  Arg [3]    : Publishing person, organization, or service, URI.

  Example    : my @avus = $ann->make_creation_metadata($time, $publisher)
  Description: Return a list of metadata AVUs describing the creation of
               an item.
  Returntype : Array[HashRef]

=cut

sub make_creation_metadata {
  my ($self, $creator, $creation_time, $publisher) = @_;

  defined $creator or
    $self->logconfess('A defined creator argument is required');
  defined $creation_time or
    $self->logconfess('A defined creation_time argument is required');
  defined $publisher or
    $self->logconfess('A defined publisher argument is required');

  return
    ($self->make_avu($DCTERMS_CREATOR,   $creator->as_string),
     $self->make_avu($DCTERMS_CREATED,   $creation_time->iso8601),
     $self->make_avu($DCTERMS_PUBLISHER, $publisher->as_string));
}

=head2 make_modification_metadata

  Arg [1]    : Modification time, DateTime.

  Example    : my @avus = $ann->make_modification_metadata($time)
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

  Example    : my @avus = $ann->make_type_metadata($sample, '.txt', '.csv')
  Description: Return an array of metadata AVUs describing the file 'type'
               (represented by its suffix).
  Returntype : Array[HashRef]

=cut

sub make_type_metadata {
  my ($self, $file, @suffixes) = @_;

  defined $file or $self->logconfess('A defined file argument is required');
  $file eq q[] and $self->logconfess('A non-empty file argument is required');

  if (not @suffixes) {
    @suffixes = @DEFAULT_FILE_SUFFIXES;
  }

  my ($suffix) = $file =~ $SUFFIX_REGEX;

  my @avus;
  if ($suffix) {
    my ($base_suffix) = $suffix =~ m{^[.]?(.*)}msx;
    $self->debug("Parsed base suffix of '$file' as '$base_suffix'");
    push @avus, $self->make_avu($FILE_TYPE, $base_suffix);
  }
  else {
    $self->debug("Did not parse a suffix from '$file'");
  }

  return @avus;
}

=head2 make_md5_metadata

  Arg [1]    : Checksum, Str.

  Example    : my @avus = $ann->make_md5_metadata($checksum)
  Description: Return an array of metadata AVUs describing the
               file MD5 checksum.
  Returntype : Array[HashRef]

=cut

sub make_md5_metadata {
  my ($self, $md5) = @_;

  defined $md5 or $self->logconfess('A defined md5 argument is required');
  $md5 eq q[] and $self->logconfess('A non-empty md5 argument is required');

  return ($self->make_avu($FILE_MD5, $md5));
}

=head2 make_ticket_metadata

  Arg [1]    : string filename

  Example    : my @avus = $ann->make_ticket_metadata($ticket_number)
  Description: Return an array of metadata AVUs describing an RT ticket
               relating to the file.
  Returntype : Array[HashRef]

=cut

sub make_ticket_metadata {
  my ($self, $ticket_number) = @_;

  defined $ticket_number or
    $self->logconfess('A defined ticket_number argument is required');
  $ticket_number eq q[] and
    $self->logconfess('A non-empty ticket_number argument is required');

  return ($self->make_avu($RT_TICKET, $ticket_number));
}

=head2 make_primary_metadata

  Arg [1]    : Run identifier, Int.
  Arg [2]    : Lane position, Int.
  Arg [3]    : Total number of reads (non-secondary/supplementary), Int.

  Named args : tag_index        Tag index, Int. Optional.
               is_paired_read   Run is paired, Bool. Optional.
               is_aligned       Run is aligned, Bool. Optional.
               reference        Reference file path, Str. Optional.
               alt_process      Alternative process name, Str. Optional.
               alignment_filter Alignment filter name, Str. Optional.

  Example    : my @avus = $ann->make_primary_metadata
                   ($id_run, $position, $num_reads,
                    tag_index      => $tag_index,
                    is_paired_read => 1,
                    is_aligned     => 1,
                    reference      => $reference)

  Description: Return a list of metadata AVUs describing a sequencing run
               component.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 4;
  my @named      = qw[tag_index is_paired_read is_aligned
                      reference alt_process alignment_filter];
  my $params = function_params($positional, @named);

  sub make_primary_metadata {
    my ($self, $id_run, $position, $num_reads) = $params->parse(@_);

    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');
    defined $num_reads or
      $self->logconfess('A defined num_reads argument is required');

    my @avus;
    push @avus, $self->make_run_metadata
      ($id_run, $position, $num_reads,
       is_paired_read => $params->is_paired_read,
       tag_index      => $params->tag_index);

    push @avus, $self->make_target_metadata($params->tag_index,
                                            $params->alignment_filter,
                                            $params->alt_process);

    push @avus, $self->make_alignment_metadata
      ($num_reads, $params->reference, $params->is_aligned,
       alignment_filter => $params->alignment_filter);

    if ($params->alt_process) {
      push @avus, $self->make_alt_metadata($params->alt_process);
    }

    return @avus;
  }
}

=head2 make_run_metadata

  Arg [1]      Run identifier, Int.
  Arg [2]      Lane position, Int.
  Arg [3]      Number of (non-secondardy/supplementary) reads present, Int.
               Optional.

  Named args:  is_paired_read  Run is paired, Bool. Required.
               tag_index       Tag index, Int.

  Example    : my @avus = $ann->make_run_metadata
                   ($id_run, $position, $num_reads,
                    is_paired_read => 1,
                    tag_index      => $tag_index);

  Description: Return HTS run metadata AVUs.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 4;
  my @named      = qw[is_paired_read tag_index];
  my $params = function_params($positional, @named);

  sub make_run_metadata {
    my ($self, $id_run, $position, $num_reads) = $params->parse(@_);

    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');
    defined $num_reads or
      $self->logconfess('A defined num_reads argument is required');

    defined $params->is_paired_read or
      $self->logconfess('A defined is_paired_read argument is required');

    my @avus = ($self->make_avu($ID_RUN,   $id_run),
                $self->make_avu($POSITION, $position));

    push @avus, $self->make_avu($TOTAL_READS, $num_reads);
    push @avus, $self->make_avu($IS_PAIRED_READ, $params->is_paired_read);

    if (defined $params->tag_index) {
      push @avus, $self->make_avu($TAG_INDEX, $params->tag_index);
    }

    return @avus;
  }
}

=head2 make_alignment_metadata

  Arg [1]      Number of (non-secondardy/supplementary) reads present, Int.
  Arg [2]      Reference file path, Str.
  Arg [3]      Run is aligned, Bool. Optional.

  Named args : alignment_filter Alignment filter name, Str. Optional.

  Example    : my @avus = $ann->make_aligment_metadata
                   ($num_reads, '/path/to/ref.fa', $is_algined,
                    alignment_filter => 'xahuman');

  Description: Return HTS alignment metadata AVUs.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 4;
  my @named      = qw[alignment_filter];
  my $params = function_params($positional, @named);

  sub make_alignment_metadata {
    my ($self, $num_reads, $reference, $is_aligned) = $params->parse(@_);

    # If there are no reads, yet the data have passed through an
    # aligner, it has been "aligned" i.e. has undergone an alignment
    # process without producing a result. However, this flag indicates
    # whether an alignment result has been obtained.
    $num_reads ||= 0;
    my $alignment = ($is_aligned and $num_reads > 0) ? 1 : 0;
    my @avus = ($self->make_avu($ALIGNMENT, $alignment));

    # A reference may be obtained from BAM/CRAM header in cases where
    # the data are not aligned e.g. where the data were aligned to
    # that reference some point in the past and have since been
    # post-processed.
    #
    # Therefore the reference metadata are only added when the
    # is_aligned argument is true.
    if ($is_aligned and $reference) {
      push @avus, $self->make_avu($REFERENCE, $reference);
    }
    if (defined $params->alignment_filter) {
      push @avus, $self->make_avu($ALIGNMENT_FILTER,
                                  $params->alignment_filter);
    }

    return @avus;
  }
}

=head2 make_target_metadata

  Arg [1]      Tag index, Int. Optional.
  Arg [1]      Alignment filter name, Str. Optional.
  Arg [1]      Alternate process name, Str. Optional.

  Example    : my @avus = $ann->make_alt_metadata('my_r&d_process');
  Description: Return HTS alternate process metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_target_metadata {
  my ($self, $tag_index, $alignment_filter, $alt_process) = @_;

  my $target = 1;
  if ((defined $tag_index and $tag_index == 0) or
      ($alignment_filter and $alignment_filter ne $YHUMAN)) {
    $target = 0;
  }
  elsif ($alt_process) {
    $target = 0;
  }

  my @avus = ($self->make_avu($TARGET, $target));
  if ($alt_process) {
    push @avus, $self->make_avu($ALT_TARGET, 1);
  }

  return @avus;
}

=head2 make_alt_metadata

  Arg [1]      Alternate process name, Str.

  Example    : my @avus = $ann->make_alt_metadata('my_r&d_process');
  Description: Return HTS alternate process metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_alt_metadata {
  my ($self, $alt_process) = @_;

  defined $alt_process or
    $self->logconfess('A defined alt_process argument is required');

  return ($self->make_avu($ALT_PROCESS, $alt_process));
}

=head2 make_secondary_metadata

  Arg [1]    : Factory for st:api::lims objects, WTSI::NPG::HTS::LIMSFactory.
  Arg [2]    : Run identifier, Int.
  Arg [3]    : Flowcell lane position, Int.

  Named args : tag_index            Tag index, Int. Optional.
               with_spiked_control  Bool.

  Example    : my @avus = $ann->make_secondary_metadata
                   ($factory, $id_run, $position, tag_index => $tag_index)

  Description: Return an array of metadata AVUs describing the HTS data
               in the specified run/lane/plex.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 4;
  my @named      = qw[tag_index with_spiked_control];
  my $params = function_params($positional, @named);

  sub make_secondary_metadata {
    my ($self, $factory, $id_run, $position) = $params->parse(@_);

    defined $factory or
      $self->logconfess('A defined factory argument is required');
    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');

    my $lims = $factory->make_lims($id_run, $position, $params->tag_index);

    my @avus;
    push @avus, $self->make_plex_metadata($lims);
    push @avus, $self->make_consent_metadata($lims);
    push @avus, $self->make_study_metadata
      ($lims, $params->with_spiked_control);
    push @avus, $self->make_sample_metadata
      ($lims, $params->with_spiked_control);
    push @avus, $self->make_library_metadata
      ($lims, $params->with_spiked_control);

    my $hts_element = sprintf 'run: %s, pos: %s, tag_index: %s',
      $id_run, $position,
      (defined $params->tag_index ? $params->tag_index : 'NA');
    $self->info("Created metadata for $hts_element: ", pp(\@avus));

    return @avus;
  }
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

  my @avus = $self->make_study_id_metadata($lims, $with_spiked_control);

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr =
    {study_accession_numbers => $STUDY_ACCESSION_NUMBER,
     study_names             => $STUDY_NAME,
     study_titles            => $STUDY_TITLE};

  push @avus, $self->_make_multi_value_metadata($lims, $method_attr,
                                                $with_spiked_control);
  return @avus
}

=head2 make_study_id_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_study_id_metadata($st);
  Description: Return HTS study_id metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_id_metadata {
  my ($self, $lims, $with_spiked_control) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {study_ids => $STUDY_ID};

  return $self->_make_multi_value_metadata($lims, $method_attr,
                                           $with_spiked_control);
}

=head2 make_sample_metadata

  Arg [1]    : A LIMS handle, st::api::lims.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my @avus = $ann->make_sample_metadata($lims);
  Description: Return HTS sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, $lims, $with_spiked_control) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr =
    {sample_accession_numbers => $SAMPLE_ACCESSION_NUMBER,
     sample_ids               => $SAMPLE_ID,
     sample_names             => $SAMPLE_NAME,
     sample_public_names      => $SAMPLE_PUBLIC_NAME,
     sample_common_names      => $SAMPLE_COMMON_NAME,
     sample_supplier_names    => $SAMPLE_SUPPLIER_NAME,
     sample_cohorts           => $SAMPLE_COHORT,
     sample_donor_ids         => $SAMPLE_DONOR_ID};

  return $self->_make_multi_value_metadata($lims, $method_attr,
                                           $with_spiked_control);
}

=head2 make_consent_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @avus = $ann->make_consent_metadata($lims);
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

  Example    : my @avus = $ann->make_library_metadata($lims);
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

  Example    : my @avus = $ann->make_plex_metadata($lims);
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
    my @values = $lims->$method_name($with_spiked_control);
    $self->debug("st::api::lims::$method_name returned ", pp(\@values));

    foreach my $value (@values) {
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

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
