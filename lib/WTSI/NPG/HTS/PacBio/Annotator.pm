package WTSI::NPG::HTS::PacBio::Annotator;

use List::AllUtils qw[uniq];
use Moose::Role;
use WTSI::NPG::iRODS::Metadata;
use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

with qw[
         WTSI::NPG::iRODS::Annotator
       ];

=head2 make_primary_metadata

  Arg [1]      PacBio run metadata, WTSI::NPG::HTS::PacBio::Metadata.
  
  Named args : data_level     Processing level of data being archived
                              e.g. Primary - off  instrument, secondary
                              - subsequently post processed. Optional.
               id_product     Product id for the data. Optional.
               is_target      Is target? If false then target flag
                              is not set. Data is not target where it
                              is not deplexed or where data at a different
                              data level is the default for the customer.
                              Boolean. Defaults to true.
               is_r_and_d     Is data R & D? Boolean. Defaults to false.
               isoseq_primers IsoSeq primers. Optional.

  Example    : my @avus = $ann->make_primary_metadata($metadata);
  Description: Return instrument, run, cell index, collection number, set
               number and sample load name AVU metadata given PacBio
               metadata from an XML file.
  Returntype : Array[HashRef]

=cut

{
    my $positional = 2;
    my @named      = qw[data_level id_product is_target is_r_and_d isoseq_primers];
    my $params     = function_params($positional, @named);

    sub make_primary_metadata {
        my ($self, $metadata) = $params->parse(@_);

        defined $metadata or
            $self->logconfess('A defined meta argument is required');

        my @avus;
        push @avus, $self->make_avu($PACBIO_CELL_INDEX,        $metadata->cell_index);
        push @avus, $self->make_avu($PACBIO_COLLECTION_NUMBER, $metadata->collection_number);
        push @avus, $self->make_avu($PACBIO_INSTRUMENT_NAME,   $metadata->instrument_name);
        push @avus, $self->make_avu($PACBIO_RUN,               $metadata->run_name);
        push @avus, $self->make_avu($PACBIO_WELL,              $metadata->well_name);
        push @avus, $self->make_avu($PACBIO_SAMPLE_LOAD_NAME,  $metadata->sample_load_name);


        if ($params->data_level) {
            push @avus, $self->make_avu($PACBIO_DATA_LEVEL, $params->data_level);
        }

        if ($metadata->plate_number) {
            push @avus, $self->make_avu($PACBIO_PLATE_NUMBER, $metadata->plate_number);
        }


        # Deprecated field, used in early version of RS
        if ($metadata->has_set_number){
            push @avus, $self->make_avu($PACBIO_SET_NUMBER, $metadata->set_number);
        }

        if ($params->is_r_and_d) {
            # R & D data
            push @avus, $self->make_avu($SAMPLE_NAME, $metadata->sample_load_name);
        }
        else {
            # Production data
            push @avus, $self->make_avu($PACBIO_SOURCE, $PACBIO_PRODUCTION);
        }

        if ($params->is_target || !defined $params->is_target) {
            push @avus, $self->make_avu($TARGET, 1);
        }

        if ($params->id_product) {
            push @avus, $self->make_avu($ID_PRODUCT, $params->id_product);
        }

        if ($params->isoseq_primers) {
            push @avus, $self->make_avu($PACBIO_ISOSEQ_PRIMERS, $params->isoseq_primers);
        }

        return @avus;
    }
}

=head2 make_secondary_metadata

  Arg [n]      PacBio run records,
               Array[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun] for
               the SMRT cell.

  Example    : my @avus = $ann->make_secondary_metadata(@run_records);
  Description: Return secondary AVU metadata for a run.
  Returntype : Array[HashRef]

=cut

sub make_secondary_metadata {
  my ($self, @run_records) = @_;

  my @avus;
  if (@run_records) {
    push @avus, $self->make_library_metadata(@run_records);
    push @avus, $self->make_study_metadata(@run_records);
    push @avus, $self->make_sample_metadata(@run_records);
    push @avus, $self->make_tag_metadata(@run_records);

    # May be removed in future if legacy data no longer required
    push @avus, $self->make_legacy_metadata(@run_records);
  }

  return @avus;
}

=head2 make_study_metadata

  Arg [n]      PacBio run records,
               Array[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun].

  Example    : my @avus = $ann->make_study_metadata(@run_records);
  Description: Return HTS study metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_metadata {
  my ($self, @run_records) = @_;

  my @studies = map { $_->study } @run_records;
  my $method_attr = {id_study_lims    => $STUDY_ID,
                     accession_number => $STUDY_ACCESSION_NUMBER,
                     name             => $STUDY_NAME,
                     study_title      => $STUDY_TITLE};

  return $self->_make_multi_value_metadata(\@studies, $method_attr);
}

=head2 make_sample_metadata

  Arg [1]    : Sample, WTSI::DNAP::Warehouse::Schema::Result::Sample.
  Example    : my @avus = $ann->make_sample_metadata($sample);
  Description: Return HTS sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, @run_records) = @_;

  my @samples = map { $_->sample } @run_records;
  my $method_attr = {accession_number => $SAMPLE_ACCESSION_NUMBER,
                     id_sample_lims   => $SAMPLE_ID,
                     id_lims          => $SAMPLE_LIMS,
                     uuid_sample_lims => $SAMPLE_UUID,
                     name             => $SAMPLE_NAME,
                     public_name      => $SAMPLE_PUBLIC_NAME,
                     common_name      => $SAMPLE_COMMON_NAME,
                     supplier_name    => $SAMPLE_SUPPLIER_NAME,
                     cohort           => $SAMPLE_COHORT,
                     donor_id         => $SAMPLE_DONOR_ID};

  return $self->_make_multi_value_metadata(\@samples, $method_attr);
}

=head2 make_library_metadata

  Arg [n]      PacBio run records,
               Array[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun].

  Example    : my @avus = $ann->make_library_metadata(@run_records);
  Description: Return library AVU metadata for a run.
  Returntype : Array[HashRef]

=cut

sub make_library_metadata {
  my ($self, @run_records) = @_;

  my $method_attr = {pac_bio_library_tube_legacy_id => $LIBRARY_ID,
                     pac_bio_library_tube_name      => $PACBIO_LIBRARY_NAME};
  my @avus = $self->_make_multi_value_metadata(\@run_records, $method_attr);

  my $num_libraries = scalar @run_records;
  if ($num_libraries > 1) {
    push @avus, $self->make_avu($PACBIO_MULTIPLEX, 1);
  }

  return @avus;
}

=head2 make_legacy_metadata

  Arg [n]      PacBio run records,
               Array[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun].

  Example    : my @avus = $ann->make_legacy_metadata($path, @run_records);
  Description: Return legacy AVU metadata for a run; study name under
               the attribute 'study_name'.
  Returntype : Array[HashRef]

=cut

sub make_legacy_metadata {
  my ($self, @run_records) = @_;

  my @avus;
  my @studies = map { $_->study } @run_records;

  my $method_attr = {name => $PACBIO_STUDY_NAME};
  push @avus, $self->_make_multi_value_metadata(\@studies, $method_attr);

  return @avus;
}

sub make_tag_metadata {
  my ($self, @run_records) = @_;

  my $method_attr = {tag_sequence   => $TAG_SEQUENCE,
                     tag_identifier => $TAG_INDEX};

  return $self->_make_multi_value_metadata(\@run_records, $method_attr);
}

=head2 make_qc_metadata

  Arg [n]      PacBio run database records,
               List[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun].
+
  Example    : my @avus = $ann->make_qc_metadata(@run_records);
  Description: Return QC outcome AVU metadata for a single product.

               An empty list is returned if the input list contains
               either no records or multiple records or the only record
               is not linked to a record in the pac_bio_product_metrics
               table.

               This method should be called in the context of a single
               iRODS object. If, according to a record in the pac_bio_run
               table, a well contains multiple samples, but in practice
               no deplexing was done, when trying to establish data
               provenance we might get multiple pac_bio_run table rows.
               Opting out of assigning a QC outcome in this case is
               a conscious conservative decision that was made at the
               time of writing (March 2024).

  Returntype : List[HashRef]

=cut

sub make_qc_metadata {
  my ($self, @run_records) = @_;

   my @avus = ();
  if (@run_records == 1) {
    my @product_metrics = $run_records[0]->pac_bio_product_metrics()->all();
    # Absence of linked product records is not unknown, one linked product
    # record is normal, multiple linked records is, most likely, an error.
    if (@product_metrics == 1) {
      my $qc_outcome = $product_metrics[0]->qc();
      if (defined $qc_outcome) {
        push @avus, $self->make_avu($QC_STATE, $qc_outcome);
      }
    }
  }

  return @avus;
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

WTSI::NPG::HTS::PacBio::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for WTSI PacBio
runs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2017, 2024 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
