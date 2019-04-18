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
         WTSI::NPG::HTS::CompositionAnnotator
         WTSI::NPG::HTS::Illumina::LIMSAnnotator
       ];

=head2 make_primary_metadata

  Arg [1]    : Biomaterial composition, npg_tracking::glossary::composition.
  Arg [2]    : Total number of reads (non-secondary/supplementary), Int.

  Named args : tag_index        Tag index, Int. Optional.
               is_paired_read   Run is paired, Bool. Optional.
               is_aligned       Run is aligned, Bool. Optional.
               reference        Reference file path, Str. Optional.
               alt_process      Alternative process name, Str. Optional.
               alignment_filter Alignment filter name, Str. Optional.
               seqchksum        Seqchksum digestgg112, Str. Optional.

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
  my $positional = 2;
  my @named      = qw[alt_process is_paired_read is_aligned
                      num_reads reference seqchksum];
  my $params = function_params($positional, @named);

  sub make_primary_metadata {
    my ($self, $composition) = $params->parse(@_);

    $composition or $self->logconfess('A composition argument is required');

    my @avus;
    my $num_reads = $params->num_reads ? $params->num_reads : 0;

    if (defined $params->num_reads) {
      push @avus, $self->make_avu($TOTAL_READS, $num_reads);
    }

    push @avus, $self->make_composition_metadata($composition);
    push @avus, $self->make_run_metadata($composition);
    foreach my $component ($composition->components_list) {
      push @avus, $self->make_target_metadata
        ($component, $params->alt_process);

      push @avus, $self->make_alignment_metadata
        ($component, $num_reads, $params->reference,
         $params->is_aligned);
    }

    push @avus, $self->make_avu($IS_PAIRED_READ,
                                 $params->is_paired_read ? 1 : 0);

    if ($params->alt_process) {
      push @avus, $self->make_alt_metadata($params->alt_process);
    }

    if ($params->seqchksum) {
      push @avus, $self->make_seqchksum_metadata($params->seqchksum);
    }

    return @avus;
  }
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

  Arg [1]      Tag index, Int. Optional.
  Arg [1]      Alignment filter name, Str. Optional.
  Arg [1]      Alternate process name, Str. Optional.

  Example    : my @avus = $ann->make_alt_metadata('my_r&d_process');
  Description: Return HTS alternate process metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_target_metadata {
  my ($self, $component, $alt_process) = @_;

  my $target = 1;
  if (($component->has_tag_index and $component->tag_index == 0) or
      ($component->has_subset and $component->subset ne $YHUMAN)) {
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

=head2 make_seqchksum_metadata

  Arg [1]      Seqchksum digest, Str.

  Example    : my @avus = $ann->make_seqchksum_metadata($digest);
  Description: Return seqchksum metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_seqchksum_metadata {
  my ($self, $digest) = @_;

  defined $digest or
    $self->logconfess('A defined alt_process argument is required');

  return ($self->make_avu($SEQCHKSUM, $digest));
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

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2017, 2018, 2019 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
