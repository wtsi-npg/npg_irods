package WTSI::NPG::HTS::ONT::Annotator;

use Moose::Role;
use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

our $GRIDION_EXPERIMENT_NAME = 'experiment_name';
our $GRIDION_DEVICE_ID       = 'device_id';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

=head2 make_primary_metadata

  Arg [1]      Experiment name, Str.
  Arg [2]      Device ID, Str.

  Example    : my @avus = $ann->make_run_metadata('Experiment 99', 'GA10000')
  Description: Return HTS run metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_primary_metadata {
  my ($self, $experiment_name, $device_id) = @_;

  defined $experiment_name or
    $self->logconfess('A defined experiment_name argument is required');
  defined $device_id or
    $self->logconfess('A defined device_id argument is required');

  my @avus;
  return ($self->make_avu($GRIDION_EXPERIMENT_NAME, $experiment_name),
          $self->make_avu($GRIDION_DEVICE_ID, $device_id));
}

sub make_secondary_metadata {
  my ($self, @run_records) = @_;

  my @avus;
  if (@run_records) {
    push @avus, $self->make_study_metadata(@run_records);
    push @avus, $self->make_sample_metadata(@run_records);
  }

  return @avus;
}

sub make_study_metadata {
  my ($self, @run_records) = @_;

  my @studies = map { $_->study } @run_records;
  my $method_attr = {id_study_lims    => $STUDY_ID,
                     accession_number => $STUDY_ACCESSION_NUMBER,
                     name             => $STUDY_NAME,
                     study_title      => $STUDY_TITLE};

  return $self->_make_multi_value_metadata(\@studies, $method_attr);
}

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

WTSI::NPG::HTS::ONT::Annotator

=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
