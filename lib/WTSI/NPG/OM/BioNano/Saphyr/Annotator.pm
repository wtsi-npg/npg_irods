package WTSI::NPG::OM::BioNano::Saphyr::Annotator;

use Moose::Role;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

our $SAPHYR_RUN_UID           = 'saphyr_run_uid';
our $SAPHYR_CHIP_SERIALNUMBER = 'saphyr_chip_serialnumber';
our $SAPHYR_CHIP_FLOWCELL     = 'saphyr_chip_flowcell';
our $SAPHYR_SAMPLE_NAME       = 'saphyr_sample_name';
our $SAPHYR_PROJECT_NAME      = 'saphyr_project_name';
our $SAPHYR_EXPERIMENT_NAME   = 'saphyr_experiment_name';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

=head2 make_primary_metadata

  Arg [1]      A Saphyr analysis job result,
               WTSI::NPG::OM::BioNano::Saphyr::JobResult

  Example    : my @avus = $ann->make_primary_metadata($job_result);
  Description: Return AVUs describing a Saphyr chip run.
  Returntype : Array[HashRef]

=cut

sub make_primary_metadata {
  my ($self, $job_result) = @_;

  defined $job_result or
    $self->logconfess('A defined job_result argument is required');

  my @avus;
  push @avus, $self->make_avu($SAPHYR_RUN_UID,
                              $job_result->chip_run_uid);
  push @avus, $self->make_avu($SAPHYR_CHIP_SERIALNUMBER,
                              $job_result->chip_serialnumber);
  push @avus, $self->make_avu($SAPHYR_CHIP_FLOWCELL,
                              $job_result->flowcell);
  push @avus, $self->make_avu($SAPHYR_SAMPLE_NAME,
                              $job_result->sample_name);
  push @avus, $self->make_avu($SAPHYR_PROJECT_NAME,
                              $job_result->project_name);
  push @avus, $self->make_avu($SAPHYR_EXPERIMENT_NAME,
                              $job_result->experiment_name);

  return @avus;
}

=head2

  Arg [n]      BmapFlowcell records,
               Array[WTSI::DNAP::Warehouse::Schema::Result::BmapFlowcell] for
               the Saphyr flowcell.

  Example    : my @avus = $ann->make_secondary_metadata(@flowcell_records);
  Description: Return secondary AVU metadata for a run.
  Returntype : Array[HashRef]
=cut

sub make_secondary_metadata {
  my ($self, @flowcell_records) = @_;

  my @avus;
  if (@flowcell_records) {
    push @avus, $self->make_library_metadata(@flowcell_records);
    push @avus, $self->make_study_metadata(@flowcell_records);
    push @avus, $self->make_sample_metadata(@flowcell_records);
  }

  return @avus;
}

=head2 make_library_metadata

  Arg [n]      BmapFlowcell records
               Array[WTSI::DNAP::Warehouse::Schema::Result::BmapFlowcell].

  Example    : my @avus = $ann->make_library_metadata(@run_records);
  Description: Return library AVU metadata for a run.
  Returntype : Array[HashRef]

=cut

sub make_library_metadata {
  my ($self, @run_records) = @_;

  my $method_attr = {id_library_lims => $LIBRARY_ID};
  my @avus = $self->_make_multi_value_metadata(\@run_records, $method_attr);

  return @avus;
}

=head2 make_sample_metadata

  Arg [1]    : Sample, WTSI::DNAP::Warehouse::Schema::Result::Sample.
  Example    : my @avus = $ann->make_sample_metadata($sample);
  Description: Return sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, @run_records) = @_;

  my @samples = map { $_->sample } @run_records;
  my $method_attr = {accession_number => $SAMPLE_ACCESSION_NUMBER,
                     id_sample_lims   => $SAMPLE_ID,
                     name             => $SAMPLE_NAME,
                     public_name      => $SAMPLE_PUBLIC_NAME,
                     common_name      => $SAMPLE_COMMON_NAME,
                     supplier_name    => $SAMPLE_SUPPLIER_NAME,
                     cohort           => $SAMPLE_COHORT,
                     donor_id         => $SAMPLE_DONOR_ID};

  return $self->_make_multi_value_metadata(\@samples, $method_attr);
}


=head2 make_study_metadata

  Arg [n]      BmapFlowcell records
               Array[WTSI::DNAP::Warehouse::Schema::Result::BmapFlowcell].

  Example    : my @avus = $ann->make_study_metadata(@flowcell_records);
  Description: Return study metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_metadata {
  my ($self, @flowcell_records) = @_;

  my @studies = map { $_->study } @flowcell_records;
  my $method_attr = {id_study_lims => $STUDY_ID};

  return $self->_make_multi_value_metadata(\@studies, $method_attr);
}

sub _make_multi_value_metadata {
  my ($self, $objs, $method_attr) = @_;
  # The method_attr argument is a map of method name to attribute name
  # under which the result will be stored.

  my @avus;
  foreach my $method_name (sort keys %{$method_attr}) {
    my $attr = $method_attr->{$method_name};
    foreach my $obj (@{$objs}) {
      my $value = $obj->$method_name;
      if (defined $value) {
        $self->debug($obj, "::$method_name returned ", $value);
        push @avus, $self->make_avu($attr, $value);
      }
      else {
        $self->debug($obj, "::$method_name returned undef");
      }
    }
  }

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for BioNano Saphyr
runs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
