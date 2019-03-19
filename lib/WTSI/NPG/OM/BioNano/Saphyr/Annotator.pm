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

sub make_secondary_metadata {
  my ($self) = @_;

  my @avus;

  # TODO: get study information from ML warehouse
  # push @avus, $self->make_avu('study_id', ????);
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
