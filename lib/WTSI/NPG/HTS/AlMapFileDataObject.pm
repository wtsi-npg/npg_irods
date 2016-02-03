package WTSI::NPG::HTS::AlMapFileDataObject;

use namespace::autoclean;
use Data::Dump qw(pp);
use List::AllUtils qw(any none uniq);
use Moose;
use Try::Tiny;

use WTSI::NPG::HTS::Types qw(AlMapFileFormat);

our $VERSION = '';

our $DEFAULT_SAMTOOLS_EXECUTABLE = 'samtools';
# Regex for matching to reference sequence paths in HTS file header PG
# records.
our $DEFAULT_REFERENCE_REGEX = qr{\/(nfs|lustre)\/\S+\/references}mxs;

our $HUMAN   = qw(human);   # FIXME
our $YHUMAN  = qw(yhuman);  # FIXME
our $XAHUMAN = qw(xahuman); # FIXME

our $PUBLIC  = qw(public);  # FIXME

extends 'WTSI::NPG::iRODS::DataObject';

with 'WTSI::NPG::HTS::RunComponent', 'WTSI::NPG::HTS::FilenameParser',
  'WTSI::NPG::HTS::HeaderParser', 'WTSI::NPG::HTS::AVUCollator',
  'WTSI::NPG::HTS::Annotator';

## no critic (ErrorHandling::RequireCheckingReturnValueOfEval)
eval { with 'npg_common::roles::software_location' };
## critic

has 'alignment_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   writer        => '_set_alignment_filter',
   documentation => 'The align filter, parsed from the iRODS path');

has 'file_format' =>
  (isa           => AlMapFileFormat,
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

# begin -- from WTSI::NPG::HTS::FilenameParser
has '+id_run' =>
  (writer        => '_set_id_run',
   documentation => 'The run ID, parsed from the iRODS path');

has '+position' =>
  (writer        => '_set_position',
   documentation => 'The position (i.e. sequencing lane), parsed ' .
                    'from the iRODS path');

has '+tag_index' =>
  (writer        => '_set_tag_index',
   documentation => 'The tag_index, parsed from the iRODS path');
# end

sub BUILD {
  my ($self) = @_;

  my ($id_run, $position, $tag_index, $alignment_filter, $file_format) =
    $self->parse_file_name($self->str);

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

  if (not defined $self->alignment_filter) {
    $self->_set_alignment_filter($alignment_filter);
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
               a filter that matches on the default reference regex,
               CodeRef.

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

  # FIXME -- this header analysis needs to be replaced

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

=head2 is_restricted_access

  Arg [1]      None

  Example    : $obj->is_restricted_access
  Description: Return true if the file contains or may contain sensitive
               information and is not for unrestricted public access.

               Access restriction is determined by two criteria; the
               alignment_filter metadata and the study_id metadata. If
               the former indicates non-consented human data or the latter
               indicates any study restriction, this method will return true.
  Returntype : Bool

=cut

sub is_restricted_access {
  my ($self) = @_;

  return $self->contains_nonconsented_human or $self->get_expected_groups;
}

=head2 contains_nonconsented_human

  Arg [1]      None

  Example    : $obj->contains_nonconsented_human
  Description: Return true if the file contains human data not having
               explicit consent. This is indicated by alignment results
               returned by the alignment_filter method.

  Returntype : Bool

=cut

sub contains_nonconsented_human {
  my ($self) = @_;

  my $af = $self->alignment_filter;
  my $contains_consented_human;
  if ($af and ($af eq $HUMAN or $af eq $XAHUMAN)) {
    $contains_consented_human = 1;
    $self->debug("$af indicates nonconsented human");
  }
  else {
    $contains_consented_human = 0;
  }

  return $contains_consented_human;
}

=head2 update_secondary_metadata

  Arg [1]    : Factory making st::api::lims, WTSI::NPG::HTS::LIMSFactory.
  Arg [2]    : HTS data has spiked control, Bool. Optional.
  Arg [3]    : Reference filter (see reference method), CoreRef. Optional.

  Example    : $obj->update_secondary_metadata($schema);
  Description: Update all secondary (LIMS-supplied) metadata and set data
               access permissions. Return $self.
  Returntype : WTSI::NPG::HTS::AlMapFileDataObject

=cut

sub update_secondary_metadata {
  my ($self, $factory, $with_spiked_control, $filter) = @_;

  defined $factory or
    $self->logconfess('A defined factory argument is required');

  my $path = $self->str;
  my @avus = $self->make_secondary_metadata
    ($factory, $self->id_run, $self->position,
     tag_index           => $self->tag_index,
     with_spiked_control => $with_spiked_control);
  $self->debug("Created metadata AVUs for '$path' : ", pp(\@avus));

  # Collate into lists of values per attribute
  my %collated_avus = %{$self->collate_avus(@avus)};

  # Sorting by attribute to allow repeated updates to be in
  # deterministic order
  my @attributes = sort keys %collated_avus;
  $self->debug("Superseding AVUs on '$path' in order of attributes: ",
               join q[, ], @attributes);
  foreach my $attr (@attributes) {
    my $values = $collated_avus{$attr};
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


before 'update_group_permissions' => sub {
  my ($self) = @_;

  # If the data contains any non-consented human data, or we are
  # expecting to set groups restricting general access, then remove
  # access for the public group.
  if ($self->is_restricted_access) {
    $self->debug(qq[Removing $PUBLIC access to '], $self->str, q[']);
    $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION, $PUBLIC);
  }
};

sub _read_header {
  my ($self) = @_;

  my $samtools;
  if ($self->can('samtools_cmd')) {
    $self->debug('Using npg_common::roles::software_location to find ',
                 'samtools: ', $self->samtools_cmd);
    $samtools = $self->samtools_cmd;
  }
  else {
    $self->debug('Using the default samtools executable on PATH: ',
                 $DEFAULT_SAMTOOLS_EXECUTABLE);
    $samtools = $DEFAULT_SAMTOOLS_EXECUTABLE;
  }

  my @header;
  my $path = $self->str;

  try {
    push @header, WTSI::DNAP::Utilities::Runnable->new
      (arguments  => [qw[view -H], qq[irods:$path]],
       executable => $samtools,
       logger     => $self->logger)->run->split_stdout;
  } catch {
    # No logger is set on samtools directly to avoid noisy stack
    # traces when a file can't be read. Instead, any error information
    # is captured here, non-fatally.

    ## no critic (RegularExpressions::RequireDotMatchAnything)
    my ($msg) = m{^(.*)$}mx;
    ## use critic
    $self->error("Failed to read the header of '$path': ", $msg);
  };

  return \@header;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::AlMapFileDataObject

=head1 DESCRIPTION

Represents an alignment/map (CRAM or BAM) file in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

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
