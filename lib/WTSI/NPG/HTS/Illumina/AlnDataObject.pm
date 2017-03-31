package WTSI::NPG::HTS::Illumina::AlnDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use Encode qw[decode];
use English qw[-no_match_vars];
use List::AllUtils qw[any];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::HTS::HeaderParser;
use WTSI::NPG::HTS::Types qw[AlnFormat];

our $VERSION = '';

our $DEFAULT_SAMTOOLS_EXECUTABLE = 'samtools_irods';

# SAM SQ header tag
our $SQ = 'SQ';

extends 'WTSI::NPG::HTS::DataObject';

with qw[
         WTSI::NPG::HTS::AlFilter
         WTSI::NPG::HTS::Illumina::RunComponent
         WTSI::NPG::HTS::Illumina::FilenameParser
       ];

## no critic (ErrorHandling::RequireCheckingReturnValueOfEval)
eval { with qw[npg_common::roles::software_location] };
## critic

has 'header' =>
  (is            => 'rw',
   isa           => 'ArrayRef[Str]',
   init_arg      => undef,
   predicate     => 'has_header',
   clearer       => 'clear_header',
   documentation => 'The HTS file header (or excerpts of it)');

has '+file_format' =>
  (isa           => AlnFormat);

has '+is_restricted_access' =>
  (is            => 'ro');

has '+primary_metadata' =>
  (is            => 'ro');

my $header_parser;

sub BUILD {
  my ($self) = @_;

  # Parsing the file name could be delayed because the parsed values
  # are not required for all operations

  # WTSI::NPG::HTS::FilenameParser
  my ($id_run, $position, $tag_index, $alignment_filter, $file_format) =
    $self->parse_file_name($self->str);

  if (not defined $self->id_run) {
    defined $id_run or
      $self->logconfess('Failed to parse id_run from path ', $self->str);
    $self->set_id_run($id_run);
  }
  if (not defined $self->position) {
    defined $position or
      $self->logconfess('Failed to parse position from path ', $self->str);
    $self->set_position($position);
  }
  if (defined $tag_index and not defined $self->tag_index) {
    $self->set_tag_index($tag_index);
  }

  if (not defined $self->alignment_filter) {
    $self->set_alignment_filter($alignment_filter);
  }

  $header_parser = WTSI::NPG::HTS::HeaderParser->new;

  # Modifying read-only attribute
  push @{$self->primary_metadata},
    $ALIGNMENT,
    $ALIGNMENT_FILTER,
    $ALT_PROCESS,
    $ALT_TARGET,
    $ID_RUN,
    $IS_PAIRED_READ,
    $POSITION,
    $REFERENCE,
    $SEQCHKSUM,
    $TAG_INDEX,
    $TARGET,
    $TOTAL_READS;

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
  my @sq = $header_parser->get_records($self->header, $SQ);
  if (@sq) {
    $self->debug($self->str, ' has SQ record: ', $sq[0]);
    $is_aligned = 1;
  }

  return $is_aligned;
}

=head2 reference

  Arg [1]      A callback to be executed on each PG line of the BAM/CRAM
               header, CodeRef. Optional, defaults to a filter that matches
               on the default reference regex.

  Example    : my $reference = $obj->reference(sub {
                  return $_[0] =~ /my_reference/
               })
  Description: Return the reference path used in alignment, if the data
               are aligned, or undef otherwise.

               The reference path is taken from the last aligner PG
               record in the PP <- PG graph, or from the last aligner
               PG line in the headeri if the graph cannot be resolved
               into a single, unbranched walk from root to leaf.

               The reference path is parsed from CL value of the PG line
               by a simple split on whitespace, followed by application
               of the filter.

               This will give incorrect results if the reference path
               contains whitespace.
  Returntype : Bool

=cut

sub reference {
  my ($self, $filter) = @_;

  my $reference;
  if ($self->is_aligned) {
    $reference = $header_parser->alignment_reference($self->header, $filter);
  }

  return $reference;
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
  my $contains_nonconsented_human;
  if ($af and ($af eq $HUMAN or $af eq $XAHUMAN)) {
    $contains_nonconsented_human = 1;
    $self->debug("$af indicates nonconsented human");
  }
  else {
    $contains_nonconsented_human = 0;
  }

  return $contains_nonconsented_human;
}

override 'update_group_permissions' => sub {
  my ($self, $strict_groups) = @_;

  if ($self->contains_nonconsented_human) {
    my $path = $self->str;

    my @groups = $self->get_groups($WTSI::NPG::iRODS::READ_PERMISSION);
    $self->info('Ensuring permissions removed for nonconsented human on ',
                "'$path': for groups ", pp(\@groups));
    my @failed_groups;

    foreach my $group (@groups) {
      try {
        $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION, $group);
      } catch {
        push @failed_groups, $group;
        $self->error("Failed to remove permissions for group '$group' from ",
                     "'$path': ", $_);
      };
    }

    my $num_groups = scalar @groups;
    my $num_errors = scalar @failed_groups;
    if ($num_errors > 0) {
      $self->logcroak("Failed to remove $num_errors / $num_groups group ",
                      "permissions from '$path': ", pp(\@failed_groups));
    }

    return $self;
  }
  else {
    return super();
  }
};

sub _build_is_restricted_access {
  my ($self) = @_;

  return 1;
}

sub _read_header {
  my ($self) = @_;

  my $samtools;
  if ($self->can('samtools_irods_cmd')) {
    $self->debug('Using npg_common::roles::software_location to find ',
                 'samtools_irods: ', $self->samtools_irods_cmd);
    $samtools = $self->samtools_irods_cmd;
  }
  else {
    $self->debug('Using the default samtools executable on PATH: ',
                 $DEFAULT_SAMTOOLS_EXECUTABLE);
    $samtools = $DEFAULT_SAMTOOLS_EXECUTABLE;
  }

  my @header;
  my $path = $self->str;

  try {
    my $run = WTSI::DNAP::Utilities::Runnable->new
      (arguments  => [qw[view -H], "irods:$path"],
       executable => $samtools)->run;

    my $stdout = ${$run->stdout};
    my $header = q[];
    try {
      $header = decode('UTF-8', $stdout, Encode::FB_CROAK);
    } catch {
      $self->warn("Non UTF-8 data in the header of '$path'");
      $header = $stdout;
    };

    push @header, split $INPUT_RECORD_SEPARATOR, $header;
  } catch {
    # No logger is set on samtools directly to avoid noisy stack
    # traces when a file can't be read.

    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak("Failed to read the header of '$path': ", pop @stack);
  };

  return \@header;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::AlnDataObject

=head1 DESCRIPTION

Represents an alignment/map (CRAM or BAM) file in iRODS. This class
overrides some base class behaviour to introduce:

 Reading of BAM/CRAM file headers.

 Custom primary metadata restrictions.

 Identification of nonconsented human data.

 Handling of the 'public' group during 'update_group_permissions' calls.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2017 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
