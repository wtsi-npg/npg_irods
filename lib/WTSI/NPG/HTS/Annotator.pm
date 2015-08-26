package WTSI::NPG::HTS::Annotator;

use List::AllUtils qw(uniq);
use Moose::Role;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::HTS::Annotation';

=head2 make_hts_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema multi-LIMS schema
  Arg [2]    : Int run identifier
  Arg [3]    : Int flowcell lane position
  Arg [4]    : Int tag index. Optional.

  Example    : $ann->make_hts_metadata($schema, 3002, 3, 1)
  Description: Return an array of metadata AVUs describing the HTS data
               in the specified run/lane/plex.
  Returntype : Array[HashRef]

=cut

sub make_hts_metadata {
  my ($self, $schema, $id_run, $position, $tag_index) = @_;

  my @query = ('me.id_run'                     => $id_run,
               'me.position'                   => $position,
               'iseq_product_metrics.id_run'   => $id_run,
               'iseq_product_metrics.position' => $position);
  if (defined $tag_index) {
    push @query, ('iseq_product_metrics.tag_index' => $tag_index)
  }

  my $run_lane_metrics = $schema->resultset('IseqRunLaneMetric')->search
    ({@query}, {prefetch => {'iseq_product_metrics' =>
                             {'iseq_flowcell' => ['sample', 'study']}}});

  my @meta;
  while (my $rlm = $run_lane_metrics->next) {
    push @meta, $self->make_run_metadata($rlm);

    # It is possible for a sequencing run without an indexing read to
    # be performed on a tagged library. The IseqProductMetric::tag_index
    # method checks for the indexing read, not for the presence of tags.

    foreach my $pm ($rlm->iseq_product_metrics) {
      if ($pm->tag_index) {
        # An indexing read was sequenced, therefore we expect a tag
        # argument to select the correct metadata
        if (not defined $tag_index) {
          $self->logconfess('Failed to make metadata for ',
                            "$id_run:$position:?",
                            'with an indexing read because no tag index ',
                            'was supplied');
        }
        else {
          $self->debug("Making metadata for $id_run:$position:$tag_index ",
                       'with an indexing read');
        }
      }
      else {
        # No indexing read was sequenced and the metadata for all tags
        # is collected (if there are tags)
        $self->debug("Making metadata for $id_run:$position ",
                     'without an indexing read');
      }

      push @meta, $self->make_study_metadata($pm);
      push @meta, $self->make_sample_metadata($pm);
      push @meta, $self->make_library_metadata($pm);
      push @meta, $self->make_plex_metadata($pm);
    }
  }

  @meta = $self->remove_duplicate_avus(@meta);

  return @meta;
}

=head2 make_run_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema::Result::IseqRunLaneMetric

  Example    : $ann->make_run_metadata($rlm);
  Description: Return HTS run metadata.
  Returntype : Array[HashRef]

=cut

sub make_run_metadata {
  my ($self, $rlm) = @_;

  my @meta;
  push @meta, $self->make_avu
    ($self->metadata_attr('id_run'), $rlm->id_run);

  push @meta, $self->make_avu
    ($self->metadata_attr('position'), $rlm->position);

  push @meta, $self->make_avu
    ($self->metadata_attr('is_paired_read'), $rlm->paired_read);

  return @meta;
}

=head2 make_study_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema::Result::IseqProductMetric

  Example    : $ann->make_study_metadata($pm);
  Description: Return HTS study metadata.
  Returntype : Array[HashRef]

=cut

sub make_study_metadata {
  my ($self, $pm) = @_;

  my @meta;
  if ($pm->iseq_flowcell) {
    if ($pm->iseq_flowcell->study) {
      push @meta, $self->make_avu
        ($self->metadata_attr('study_id'),
         $pm->iseq_flowcell->study->id_study_lims);

      if ($pm->iseq_flowcell->study->name) {
        push @meta, $self->make_avu
          ($self->metadata_attr('study'),
           $pm->iseq_flowcell->study->name);
      }
      if ($pm->iseq_flowcell->study->accession_number) {
        push @meta, $self->make_avu
          ($self->metadata_attr('study_accession_number'),
           $pm->iseq_flowcell->study->accession_number);
      }
      if ($pm->iseq_flowcell->study->study_title) {
        push @meta, $self->make_avu
          ($self->metadata_attr('study_title'),
           $pm->iseq_flowcell->study->study_title);
      }
    }
  }

  return @meta;
}

=head2 make_sample_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema::Result::IseqProductMetric

  Example    : $ann->make_sample_metadata($pm);
  Description: Return HTS sample metadata.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, $pm) = @_;

  my @meta;
  if ($pm->iseq_flowcell) {
    if ($pm->iseq_flowcell->sample->public_name) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_public_name'),
         $pm->iseq_flowcell->sample->public_name);
    }
    if ($pm->iseq_flowcell->sample->common_name) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_common_name'),
         $pm->iseq_flowcell->sample->common_name);
    }
    if ($pm->iseq_flowcell->sample->supplier_name) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_supplier_name'),
         $pm->iseq_flowcell->sample->supplier_name);
    }
    if ($pm->iseq_flowcell->sample->cohort) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_cohort'),
         $pm->iseq_flowcell->sample->cohort);
    }
    if ($pm->iseq_flowcell->sample->donor_id) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_donor_id'),
         $pm->iseq_flowcell->sample->donor_id);
    }
    if ($pm->iseq_flowcell->sample->consent_withdrawn) {
      push @meta, $self->make_avu
        ($self->metadata_attr('sample_consent_withdrawn'),
         $pm->iseq_flowcell->sample->consent_withdrawn);
    }
  }

  return @meta;
}

=head2 make_library_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema::Result::IseqProductMetric

  Example    : $ann->make_library_metadata($pm);
  Description: Return HTS library metadata.
  Returntype : Array[HashRef]

=cut

sub make_library_metadata {
  my ($self, $pm) = @_;

  my @meta;
  if ($pm->iseq_flowcell) {
    if ($pm->iseq_flowcell->library_id) {
      push @meta, $self->make_avu
        ($self->metadata_attr('library_id'), $pm->iseq_flowcell->library_id);
    }
  }

  return @meta;
}

=head2 make_plex_metadata

  Arg [1]    :  WTSI::DNAP::Warehouse::Schema::Result::IseqProductMetric

  Example    : $ann->make_plex_metadata($pm);
  Description: Return HTS plex metadata.
  Returntype : Array[HashRef]

=cut

sub make_plex_metadata {
  my ($self, $pm) = @_;

  my @meta;
  if ($pm->iseq_flowcell) {
    push @meta, $self->make_avu
      ($self->metadata_attr('tag_index'), $pm->iseq_flowcell->tag_index);

    push @meta, $self->make_avu
      ($self->metadata_attr('manual_qc'), $pm->iseq_flowcell->manual_qc);
  }

  return @meta;
}

sub make_avu {
  my ($self, $attribute, $value, $units) = @_;
  return {attribute => $attribute,
          value     => $value,
          units     => $units};
}

sub remove_duplicate_avus {
  my ($self, @meta) = @_;

  my %meta_tree;
  foreach my $avu (@meta) {
    my $a = $avu->{attribute};
    my $u = $avu->{units} || q[]; # Empty string as proxy for undef

    if (exists $meta_tree{$a}{$u}) {
      push @{$meta_tree{$a}{$u}}, $avu->{value}
    }
    else {
      $meta_tree{$a}{$u} = [$avu->{value}]
    }
  }

  my @uniq;
  foreach my $a (sort keys %meta_tree) {
    foreach my $u (sort keys $meta_tree{$a}) {
      my @values = uniq @{$meta_tree{$a}{$u}};

      foreach my $v (sort @values) {
        push @uniq, $self->make_avu($a, $v, $u ? $u : undef);
      }
    }
  }

  return @uniq;
}

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
