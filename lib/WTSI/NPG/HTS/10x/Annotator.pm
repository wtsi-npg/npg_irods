package WTSI::NPG::HTS::10x::Annotator;

use Data::Dump qw[pp];
use Moose::Role;

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;
use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

our $SEQCHKSUM = 'seqchksum'; # FIXME -- move to WTSI::NPG::iRODS::Metadata

with qw[
         WTSI::NPG::HTS::Annotator
       ];

=head2 make_primary_metadata

  Arg [1]    : Run identifier, Int.
  Arg [2]    : Lane position, Int.
  Arg [3]    : Read, Str.
  Arg [4]    : Tag sequence, Str.

  Named args : alt_process      Alte
    rnative process name, Str. Optional.
               seqchksum        Seqchksum digest, Str. Optional.

  Example    : my @avus = $ann->make_primary_metadata
                   ($id_run, $position, $read, $tag)

  Description: Return a list of metadata AVUs describing a sequencing run
               component.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 5;
  my @named      = qw[alt_process seqchksum];
  my $params = function_params($positional, @named);

  sub make_primary_metadata {
    my ($self, $id_run, $position, $read, $tag) = $params->parse(@_);

    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');
    defined $read or
      $self->logconfess('A defined read argument is required');
    defined $tag or
      $self->logconfess('A defined tag argument is required');

    my @avus;
    push @avus, $self->make_run_metadata
      ($id_run, $position, $read, $tag);

    if ($params->alt_process) {
      push @avus, $self->make_alt_metadata($params->alt_process);
    }

    if ($params->seqchksum) {
      push @avus, $self->make_seqchksum_metadata($params->seqchksum);
    }

    my $hts_element = sprintf 'run: %s, pos: %s, read: %s, tag %s',
    $id_run, $position, $read, $tag;
    $self->debug("Created primary metadata for $hts_element: ", pp(\@avus));

    return @avus;
  }
}

=head2 make_run_metadata

  Arg [1]      Run identifier, Int.
  Arg [2]      Lane position, Int.
  Arg [3]      Read, Str.
  Arg [4]      Tag sequence, Str.

  Named args:  is_paired_read  Run is paired, Bool. Required.
               tag       Tag sequence, String.

  Example    : my @avus = $ann->make_run_metadata
                   ($id_run, $position, $read, $tag);

  Description: Return HTS run metadata AVUs.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 5;
  my @named      = qw[tag];
  my $params = function_params($positional, @named);

  sub make_run_metadata {
    my ($self, $id_run, $position, $read, $tag) = $params->parse(@_);

    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');
    defined $read or
      $self->logconfess('A defined read argument is required');
    defined $tag or
      $self->logconfess('A defined tag argument is required');

    my @avus = ($self->make_avu($ID_RUN,   $id_run),
                $self->make_avu($POSITION, $position),
                $self->make_avu($READ, $read),
                $self->make_avu($TAG, $tag),
        );

    return @avus;
  }
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
    $self->logconfess('A defined digest argument is required');

  return ($self->make_avu($SEQCHKSUM, $digest));
}

=head2 make_secondary_metadata

  Arg [1]    : Factory for st:api::lims objects, WTSI::NPG::HTS::LIMSFactory.
  Arg [2]    : Run identifier, Int.
  Arg [3]    : Flowcell lane position, Int.
  Arg [3]    : Read, Str.
  Arg [3]    : Tag, Str

  Example    : my @avus = $ann->make_secondary_metadata
                   ($factory, $id_run, $position, $read, $tag)

  Description: Return an array of metadata AVUs describing the HTS data
               in the specified run/lane/plex.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 6;
  my $params = function_params($positional);

  sub make_secondary_metadata {
    my ($self, $factory, $id_run, $position, $read, $tag) = $params->parse(@_);

    defined $factory or
      $self->logconfess('A defined factory argument is required');
    defined $id_run or
      $self->logconfess('A defined id_run argument is required');
    defined $position or
      $self->logconfess('A defined position argument is required');
    defined $read or
      $self->logconfess('A defined read argument is required');
    defined $tag or
      $self->logconfess('A defined tag argument is required');

    my $lims = $factory->make_lims($id_run, $position);
    ($lims) = grep { $_->tag_sequence eq $tag } $lims->children;
    unless( defined $lims ) {
      $lims = $factory->make_lims($id_run, $position);
    }
    defined $lims or 
      $self->logconfess("Failed to create st::api::lims for run $id_run ",
                        "lane $position read $read tag $tag");

    my @avus;
    push @avus, $self->make_plex_metadata($lims);
    push @avus, $self->make_consent_metadata($lims);
    push @avus, $self->make_study_metadata($lims);
    push @avus, $self->make_sample_metadata($lims);
    push @avus, $self->make_library_metadata($lims);

    my $hts_element = sprintf 'run: %s, pos: %s, read: %s, tag: %s',
      $id_run, $position, $read, $tag;
    $self->debug("Created metadata for $hts_element: ", pp(\@avus));

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
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  my @avus = $self->make_study_id_metadata($lims);

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr =
    {study_accession_numbers => $STUDY_ACCESSION_NUMBER,
     study_names             => $STUDY_NAME,
     study_titles            => $STUDY_TITLE};

  push @avus, $self->_make_multi_value_metadata($lims, $method_attr);
  return @avus
}

=head2 make_study_id_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @meta = $ann->make_study_id_metadata($st);
  Description: Return HTS study_id metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_id_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {study_ids => $STUDY_ID};

  return $self->_make_multi_value_metadata($lims, $method_attr);
}

=head2 make_sample_metadata

  Arg [1]    : A LIMS handle, st::api::lims.

  Example    : my @avus = $ann->make_sample_metadata($lims);
  Description: Return HTS sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, $lims) = @_;

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

  return $self->_make_multi_value_metadata($lims, $method_attr);
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

  Example    : my @avus = $ann->make_library_metadata($lims);
  Description: Return HTS library metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_library_metadata {
  my ($self, $lims) = @_;

  defined $lims or $self->logconfess('A defined lims argument is required');

  # Map of method name to attribute name under which the result will
  # be stored.
  my $method_attr = {library_ids   => $LIBRARY_ID,
                     library_types => $LIBRARY_TYPE};

  return $self->_make_multi_value_metadata($lims, $method_attr);
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
  my $method_attr =
  {tag_index => $TAG_INDEX,
     qc_state  => $QC_STATE};

  return $self->_make_multi_value_metadata($lims, $method_attr);
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
      if (defined $value) {
        push @avus, $self->make_avu($attr, $value);
      }
    }
  }

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10x::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for WTSI 10x
runs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

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
