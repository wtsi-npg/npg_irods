package WTSI::NPG::HTS::HTSFileDataObject;

use namespace::autoclean;
use Data::Dump qw(pp);
use List::AllUtils qw(any none uniq);
use Moose;
use Try::Tiny;

use WTSI::NPG::HTS::Samtools;
use WTSI::NPG::HTS::Types qw(HTSFileFormat);
use WTSI::NPG::iRODS::Metadata qw($ALIGNMENT $REFERENCE);

our $VERSION = '';

# Regex for matching to reference sequence paths in HTS file header PG
# records.
our $DEFAULT_REFERENCE_REGEX = qr{\/(nfs|lustre)\/\S+\/references}mxs;

extends 'WTSI::NPG::iRODS::DataObject';

with 'WTSI::NPG::HTS::HeaderParser', 'WTSI::NPG::HTS::Annotator';

has 'align_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   writer        => '_set_align_filter',
   documentation => 'The align filter, parsed from the iRODS path');

has 'file_format' =>
  (isa           => HTSFileFormat,
   is            => 'ro',
   required      => 0,
   writer        => '_set_file_format',
   documentation => 'The storage format of the file');

has 'header' =>
  (is            => 'rw',
   isa           => 'ArrayRef[Str]',
   init_arg      => undef,
   predicate     => 'has_header',
   clearer       => 'clear_header',
   documentation => 'The HTS file header (or excerpts of it)');

has 'id_run' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 0,
   writer        => '_set_id_run',
   documentation => 'The run ID, parsed from the iRODS path');

has 'position' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 0,
   writer        => '_set_position',
   documentation => 'The position (i.e. sequencing lane), parsed ' .
                    'from the iRODS path');

has 'tag_index' =>
  (isa           => 'Maybe[Int]',
   is            => 'ro',
   required      => 0,
   writer        => '_set_tag_index',
   documentation => 'The tag_index, parsed from the iRODS path');

sub BUILD {
  my ($self) = @_;

  my ($id_run, $position, $tag_index, $align_filter, $file_format) =
    $self->_parse_file_name;

  if (not defined $self->id_run) {
    defined $id_run or
      $self->logconfess('Failed to parse id_run from path ', $self->str);
    $self->_set_id_run($id_run);
  }

  if (not defined $self->position) {
    defined $position or
      $self->logconfess('Failed to parse position from path ', $self->str);
    $self->_set_position($position);
  }

  if (not defined $self->file_format) {
    defined $file_format or
      $self->logconfess('Failed to parse file format from path ', $self->str);
    $self->_set_file_format($file_format);
  }

  if (not defined $self->align_filter) {
    $self->_set_align_filter($align_filter);
  }

  if (not defined $self->tag_index) {
    $self->_set_tag_index($tag_index);
  }

  return;
}

# Lazily load header from iRODS
around 'header' => sub {
  my ($orig, $self) = @_;

  if (not $self->has_header) {
    my $header = $self->_read_header;
    $self->$orig($header);
  }

  return $self->$orig;
};

=head2 is_aligned

  Arg [1]      None

  Example    : $obj->is_aligned
  Description: Return true if the reads in the file are aligned.
  Returntype : Bool

=cut

sub is_aligned {
  my ($self) = @_;

  my $is_aligned = 0;
  my @sq = $self->get_records($self->header, 'SQ');
  if (@sq) {
    $self->debug($self->str, ' has SQ record: ', $sq[0]);
    $is_aligned = 1;
  }

  return $is_aligned;
}

=head2 reference

  Arg [1]      A callback to be executed on each line of BWA @PG line
               of the BAM/CRAM header, CodeRef. Optional, defaults to
               a filter that matches on the default reference regex.

  Example    : my $reference = $obj->reference(sub {
                  return $_[0] =~ /my_reference/
               })
  Description: Return the reference path used in alignment. The reference
               path is parsed from the last BWA @PG line in the header
               by a simple split on whitespace, followed by application of
               the filter.

               This will give incorrect results if the reference path
               contains whitespace.

  Returntype : Bool

=cut

sub reference {
  my ($self, $filter) = @_;

  defined $filter and ref $filter ne 'CODE' and
    $self->logconfess('The filter argument must be a CodeRef');

  $filter ||= sub {
    return $_[0] =~ m{$DEFAULT_REFERENCE_REGEX}msx;
  };

  # This filter was cribbed from the existing code. I don't understand
  # why the bwa_sam header ID record is significant.
  my $last_bwa_pg_line;

  foreach my $header_record ($self->get_records($self->header, 'PG')) {
    my @id = $self->get_values($header_record, 'ID');
    my @cl = $self->get_values($header_record, 'CL');

    if (any  { m{^bwa}msx     } @id and
        none { m{^bwa_sam}msx } @id and
        @cl) {
      $self->debug($self->str, " has BWA PG line: $header_record");
      $last_bwa_pg_line = $header_record;
    }
  }

  my $reference;
  if ($last_bwa_pg_line) {
    # Note the uniq filter; the correct reference may appear multiple
    # times in the PG header record's CL values.
    my @elts;
    foreach my $cl ($self->get_values($last_bwa_pg_line, 'CL')) {
      push @elts, split m{\s+}msx, $cl;
    }

    @elts = uniq grep { $filter->($_) } @elts;
    my $num_elts = scalar @elts;

    if ($num_elts == 1) {
      $self->debug(q[Found reference '], $elts[0], q[' for ], $self->str);
      $reference = $elts[0];
    }
    elsif ($num_elts > 1) {
      $self->logconfess("Reference filter matched $num_elts elements in PG ",
                        "record '$last_bwa_pg_line': ", pp(\@elts));
    }
  }

  return $reference;
}

=head2 update_secondary_metadata

  Arg [1]    : WTSI::DNAP::Warehouse::Schema multi-LIMS schema
  Arg [2]    : CodeRef reference filter (see reference method). Optional.

  Example    : $obj->update_secondary_metadata($schema);
  Description: Update all secondary (LIMS-supplied) metadata and set data
               access permissions. Return $self.
  Returntype : WTSI::NPG::HTS::HTSFileDataObject

=cut

sub update_secondary_metadata {
  my ($self, $schema, $filter) = @_;

  my @meta = $self->make_hts_metadata($schema,
                                      $self->id_run,
                                      $self->position,
                                      $self->tag_index);
  push @meta, $self->make_avu($ALIGNMENT, $self->is_aligned);
  push @meta, $self->make_avu($REFERENCE, $self->reference($filter));

  $self->debug('Created metadata AVUs: ', pp(\@meta));

  # Collate into lists of values per attribute
  my %collated_avus;
  foreach my $avu (@meta) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    if (exists $collated_avus{$attr}) {
      push @{$collated_avus{$attr}}, $value;
    }
    else {
      $collated_avus{$attr} = [$value];
    }
  }

  $self->debug('Collated ', scalar @meta, ' AVUs into ',
               scalar keys %collated_avus, ' lists');
  $self->debug('Superseding AVUs in order of attributes: ',
               join q[, ], keys %collated_avus);

  # Sorting by attribute to allow repeated updates to be in
  # deterministic order
  foreach my $attr (sort keys %collated_avus) {
    my $values  = $collated_avus{$attr};
    try {
      $self->supersede_multivalue_avus($attr, $values, undef);
    } catch {
      $self->error("Failed to supersede with attribute '$attr' and values ",
                   pp($values), q[: ], $_);
    };
  }

  $self->update_group_permissions;

  return $self;
}

sub _parse_file_name {
  my ($self) = @_;

  my ($id_run, $position, $align_filter, $align_filter2,
      $tag_index, $tag_index2, $align_filter3, $align_filter4, $format) =
        $self->str =~ m{\/
                        (\d+)        # Run ID
                        _            # Separator
                        (\d)         # Position
                        (_(\w+))?    # Align filter 1/2
                        (\#(\d+))?   # Tag index
                        (_(\w+))?    # Align filter 3/4
                        [.](\S+)$    # File format
                     }mxs;

  $align_filter2 ||= $align_filter4;

  return ($id_run, $position, $tag_index2, $align_filter2, $format);
}

sub _read_header {
  my ($self) = @_;

  my @header = WTSI::NPG::HTS::Samtools->new
    (arguments  => ['-H'],
     path       => 'irods:' . $self->str,
     logger     => $self->logger)->collect;

  return \@header;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::HTSFileDataObject

=head1 DESCRIPTION

Represents CRAM and BAM files in iRODS.

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
