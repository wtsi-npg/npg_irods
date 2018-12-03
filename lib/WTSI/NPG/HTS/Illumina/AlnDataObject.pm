package WTSI::NPG::HTS::Illumina::AlnDataObject;

use namespace::autoclean;
use Carp;
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

extends 'WTSI::NPG::HTS::Illumina::DataObject';

## no critic (ErrorHandling::RequireCheckingReturnValueOfEval)
eval { with qw[npg_common::roles::software_location] };
## critic

has 'header' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   builder       => '_build_header',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'The HTS file header (or excerpts of it)');

has 'is_paired_read' =>
  (is            => 'ro',
   isa           => 'Bool',
   required      => 1,
   builder       => '_build_is_paired_read',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'True if the HTS file contains paired read data');

has 'num_reads' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 0,
   predicate     => 'has_num_reads',
   documentation => 'The number of aligned reads');

has '+composition' =>
  (builder       => '_build_composition',
   lazy          => 1);

has '+file_format' =>
  (isa           => AlnFormat);

has '+is_restricted_access' =>
  (is            => 'ro');

has '+primary_metadata' =>
  (is            => 'ro');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata},
    $ALIGNMENT,
    $ALIGNMENT_FILTER,
    $ALT_PROCESS,
    $ALT_TARGET,
    $COMPONENT,
    $COMPOSITION,
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

=head2 is_aligned

  Arg [1]      None

  Example    : $obj->is_aligned
  Description: Return true if the reads in the file are aligned.
  Returntype : Bool

=cut

sub is_aligned {
  my ($self) = @_;

  my $is_aligned = 0;
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @sq = $parser->get_records($self->header, $SQ);
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
    my $parser = WTSI::NPG::HTS::HeaderParser->new;
    $reference = $parser->alignment_reference($self->header, $filter);
  }

  return $reference;
}

=head2 contains_nonconsented_human

  Arg [1]      None

  Example    : $obj->contains_nonconsented_human
  Description: Return true if the file contains human data not having
               explicit consent. This is indicated by alignment results
               returned by the subset method.

  Returntype : Bool

=cut

sub contains_nonconsented_human {
  my ($self) = @_;

  my $contains_nonconsented_human =
    any { $_->has_subset and ($_->subset eq $HUMAN or
                              $_->subset eq $XAHUMAN)
        } $self->composition->components_list;

  if ($contains_nonconsented_human) {
    $self->debug('subset indicates onconsented human sequence in ',
                 $self->str);
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

sub _build_composition {
  my ($self) = @_;

  my $path = $self->str;
  if (not $self->is_present) {
    $self->logconfess('Failed to build the composition attribute from the ',
                      "iRODS metadata of '$path' because the data object is ",
                      'not currently (or yet) stored in iRODS');
  }
  if (not $self->find_in_metadata($COMPOSITION)) {
    $self->logconfess('Failed to build the composition attribute from the ',
                      "iRODS metadata of '$path' because the data object in ",
                      "iRODS does not have a '$COMPOSITION' AVU");
  }

  my $json = $self->get_avu($COMPOSITION)->{value};

  my $pkg = 'npg_tracking::glossary::composition::component::illumina';
  return npg_tracking::glossary::composition->thaw
    ($json, component_class => $pkg);
}

sub _build_is_restricted_access {
  my ($self) = @_;

  return 1;
}

sub _build_header {
  my ($self) = @_;

  my $samtools = $self->_find_samtools;
  my $path     = $self->str;

  my @header;
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

sub _build_is_paired_read {
  my ($self) = @_;

  my $read_count = 1024;
  my @reads = $self->_get_reads($read_count);

  my $is_paired_read = 0;
  foreach my $read (@reads) {
    my ($qname, $flag, $rname, $pos) = split /\t/msx, $read;

    if (vec $flag, 0, 1) { # 0x1 == read paired
      $is_paired_read = 1;
      last;
    }
  }

  return $is_paired_read;
}

sub _get_reads {
  my ($self, $num_records) = @_;

  $num_records ||= 1024;

  my $samtools = $self->_find_samtools;
  my $path     = $self->str;

  my @reads;
  try {
    local $SIG{PIPE} = sub {
      # Without this handler, the child process some times gets
      # SIGPIPE and sometimes exits with an error. With this handler,
      # the child process always gets SIGPIPE.
    };

    open my $fh, q[-|], "$samtools view irods:$path" or
      croak "Failed to open pipe from samtools: $ERRNO";

    my $n = 0;
    while ($n < $num_records) {
      my $line = <$fh>;
      if ($line) {
        push @reads, $line;
      }
      $n++;
    }

    my $retval = close $fh;

    ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
    my $status = $CHILD_ERROR;
    my $signal = $status & 127;
    my $error  = $status >> 8;

    if ($signal) {
      if ($signal != 13) {
        # 13 == SIGPIPE
        croak "Error reading from samtools, signal: $signal";
      }
    }
    elsif ($error) {
      croak "Error reading from samtools, error: $error";
    }
    ## use critic
  } catch {
    $self->logcroak("Failed to get reads from '$path': $_");
  };

  my $num_read = scalar @reads;
  $self->debug("Read $num_read reads from '$path'");

  if ($self->has_num_reads) {
    my $num_reads = $self->num_reads;
    if ($num_reads > 0 and $num_read == 0) {
      $self->logcroak("Failed to get reads from '$path' ",
                      'with samtools, when it is recorded ',
                      "as containing $num_reads reads");
    }
  }

  return @reads;
}

sub _find_samtools {
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

  return $samtools;
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

Copyright (C) 2015, 2016, 2017, 2018 Genome Research Limited. All Rights
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
