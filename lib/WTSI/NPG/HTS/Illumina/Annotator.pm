package WTSI::NPG::HTS::Illumina::Annotator;

use Data::Dump qw[pp];
use Moose::Role;
use List::MoreUtils qw[uniq];

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;
use WTSI::DNAP::Utilities::Params qw[function_params];

use npg_tracking::glossary::composition;

our $VERSION = '';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

=head2 make_primary_metadata

  Arg [1]    : Biomaterial composition, npg_tracking::glossary::composition.

  Named args : is_paired_read   Run is paired, Bool. Optional.
               is_aligned       Run is aligned, Bool. Optional.
               reference        Reference file path, Str. Optional.
               alt_process      Alternative process name, Str. Optional.
               num_reads        Total number of reads
                                (non-secondary/supplementary), Int. Optional.
               seqchksum        Seqchksum digestgg112, Str. Optional.
               lims_factory     Factory for st:api::lims objects,
                                WTSI::NPG::HTS::LIMSFactory. Optional.

  Example    : my @avus = $ann->make_primary_metadata
                   ($composition,
                    num_reads      => 100,
                    tag_index      => $tag_index,
                    is_paired_read => 1,
                    is_aligned     => 1,
                    reference      => $reference)

  Description: Return a list of metadata AVUs describing a sequencing run
               component.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 2;
  my @named      = qw[alt_process is_paired_read is_aligned
                      lims_factory num_reads reference seqchksum];
  my $params = function_params($positional, @named);

  sub make_primary_metadata {
    my ($self, $composition) = $params->parse(@_);

    $composition or $self->logconfess('A composition argument is required');
    $self->_validate_composition($composition);

    my @avus;
    my $num_reads = $params->num_reads ? $params->num_reads : 0;

    if (defined $params->num_reads) {
      push @avus, $self->make_avu($TOTAL_READS, $num_reads);
    }

    push @avus, $self->make_composition_metadata($composition);
    push @avus, $self->make_run_metadata($composition);

    my $component = $composition->get_component(0);
    push @avus, $self->make_target_metadata($component, $params->alt_process);
    push @avus, $self->make_alignment_metadata($component,
      $num_reads, $params->reference, $params->is_aligned);

    push @avus, $self->make_avu($IS_PAIRED_READ,
                                $params->is_paired_read ? 1 : 0);

    if ($params->seqchksum) {
      push @avus, $self->make_avu($SEQCHKSUM, $params->seqchksum);
    }

    if ($params->lims_factory) {
      my $lims = $params->lims_factory->make_lims($composition);
      push @avus, $self->make_gbs_metadata($lims);
    }

    return @avus;
  }
}

=head2 make_composition_metadata

  Arg [1]      npg_tracking::glossary::composition object

  Example    : my @avus = $ann->make_composition_metadata ($composition);

  Description: Returns composition and its componets metadata AVUs.
  Returntype : Array[HashRef]

=cut 

sub make_composition_metadata {
  my ($self, $composition) = @_;

  $composition or $self->logconfess('A composition argument is required');

  my @avus;
  push @avus, $self->make_avu($COMPOSITION, $composition->freeze);
  push @avus, $self->make_avu($ID_PRODUCT, $composition->digest);
  foreach my $component ($composition->components_list) {
    push @avus, $self->make_avu($COMPONENT, $component->freeze);
  }

  return @avus;
}

=head2 make_run_metadata

  Arg [1]      composition, npg_tracking::glossary::composition

  Example    : my @avus = $ann->make_run_metadata ($composition);

  Description: Return HTS run metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_run_metadata {
  my ($self, $composition) = @_;

  $composition or $self->logconfess('A composition argument is required');

  my @id_runs   = ();
  my @positions = ();
  my @tis       = ();
  my $uti = -1;
  foreach my $c ($composition->components_list) {
    push @id_runs, $c->id_run;
    push @positions, $c->position;
    push @tis, defined $c->tag_index ? $c->tag_index : $uti;
  }
  @id_runs   = uniq @id_runs;
  @positions = uniq @positions;
  @tis       = uniq @tis;

  my @avus;
  if (scalar @id_runs == 1) {
    push @avus, $self->make_avu($ID_RUN, $id_runs[0]),
  }
  if (scalar @positions == 1) {
    push @avus, $self->make_avu($POSITION, $positions[0]);
  }
  if ((scalar @tis == 1) && ($tis[0] != $uti)) {
    push @avus, $self->make_avu($TAG_INDEX, $tis[0]);
  }

  return @avus;
}

=head2 make_alignment_metadata

  Arg [1]      npg_tracking::glossary::composition::component::illumina object
  Arg [2]      Number of (non-secondardy/supplementary) reads present, Int.
  Arg [3]      Reference file path, Str.
  Arg [4]      Data is aligned, Bool. Optional.

  Named args : alignment_filter Alignment filter name, Str. Optional.

  Example    : my @avus = $ann->make_aligment_metadata
                   ($component, $num_reads, '/path/to/ref.fa', $is_algined);

  Description: Return HTS alignment metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_alignment_metadata {
  my ($self, $component, $num_reads, $reference, $is_aligned) = @_;

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
  if ($component->has_subset) {
    push @avus, $self->make_avu($ALIGNMENT_FILTER, $component->subset);
  }

  return @avus;
}

=head2 make_target_metadata

  Arg [1]      npg_tracking::glossary::composition::component::illumina object
  Arg [2]      Alternate process name, Str. Optional.

  Example    : my @avus = $ann->make_target_metadata($component, 'r&d_process');
  Description: Returns values for C<target> and, optionally, C<alt_target> and
               C<alt_process> metadata.
  Returntype : Array[HashRef]

=cut

sub make_target_metadata {
  my ($self, $component, $alt_process) = @_;

  my $target = 1;
  if (($component->has_tag_index and $component->tag_index == 0) or
    ($component->has_subset and $component->subset ne $YHUMAN)) {
    $target = 0;
  }

  my @avus = ($self->make_avu($TARGET, $alt_process ? 0 : $target));
  if ($alt_process) {
    if ($target) {
      push @avus, $self->make_avu($ALT_TARGET, 1);
    }
    push @avus, $self->make_avu($ALT_PROCESS, $alt_process);
  }

  return @avus;
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
  my $positional = 3;
  my @named      = qw[with_spiked_control];
  my $params = function_params($positional, @named);

  sub make_secondary_metadata {
    my ($self, $composition, $factory) = $params->parse(@_);

    $composition or $self->logconfess('A composition argument is required');
    defined $factory or
      $self->logconfess('A defined factory argument is required');

    my $lims = $factory->make_lims($composition);

    my @avus;
    push @avus, $self->make_plex_metadata($lims);
    push @avus, $self->make_consent_metadata($lims);
    push @avus, $self->make_study_metadata
      ($lims, $params->with_spiked_control);
    push @avus, $self->make_study_id_metadata
      ($lims, $params->with_spiked_control);
    push @avus, $self->make_sample_metadata
      ($lims, $params->with_spiked_control);
    push @avus, $self->make_library_metadata
      ($lims, $params->with_spiked_control);

    my $rpt = $composition->freeze2rpt;
    $self->debug("Created metadata for '$rpt': ", pp(\@avus));

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
     sample_lims              => $SAMPLE_LIMS,
     sample_names             => $SAMPLE_NAME,
     sample_public_names      => $SAMPLE_PUBLIC_NAME,
     sample_common_names      => $SAMPLE_COMMON_NAME,
     sample_supplier_names    => $SAMPLE_SUPPLIER_NAME,
     sample_uuids             => $SAMPLE_UUID,
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
  my $method_attr = {library_ids   => $LIBRARY_ID,
                     library_types => $LIBRARY_TYPE};

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

=head2 make_gbs_metadata

  Arg [1]    :  A LIMS handle, st::api::lims.

  Example    : my @avus = $ann->make_gbs_metadata($lims);
  Description: Return HTS gbs plex metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_gbs_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {gbs_plex_name => $GBS_PLEX_NAME};
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

sub _validate_composition {
  my ($self, $composition) = @_;

  my @subsets = uniq map { $_->subset } $composition->components_list();
  if (@subsets > 1) {
    $self->logconfess('Different subset values in '.$composition->freeze);
  }
  my @tag0_indexes = grep { $_ == 0 }
                     grep { defined }
                     map { $_->tag_index } $composition->components_list();
  if (@tag0_indexes && (@tag0_indexes != $composition->num_components)) {
    $self->logconfess(
      'A mixture of tag zero and other indexes in '.$composition->freeze);
  }
  return;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for WTSI Illumina
runs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

Marina Gourtovaia <mg8@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2017, 2018, 2019, 2024, 2025 GRL.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
