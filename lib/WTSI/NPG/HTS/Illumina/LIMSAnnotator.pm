package WTSI::NPG::HTS::Illumina::LIMSAnnotator;

use Data::Dump qw[pp];
use Moose::Role;

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;
use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

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
  Description: Return HTS plex metadata AVUs.
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

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::LIMSAnnotator

=head1 DESCRIPTION



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
