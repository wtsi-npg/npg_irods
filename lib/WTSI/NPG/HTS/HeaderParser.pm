package WTSI::NPG::HTS::HeaderParser;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[any first_index firstval uniq];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

# SAM header record tags
my $ID = 'ID';
my $CL = 'CL';
my $PG = 'PG';
my $PP = 'PP';

# These regexes match the supported aligners from header PG records
my $ALIGNER_BWA    = qr{^bwa(?!_sam)}msx;
my $ALIGNER_TOPHAT = qr{^TopHat}msx;
my $ALIGNER_STAR   = qr{^STAR}msx;
my $ALIGNER_MINIMAP2 = qr{^minimap2}msx;
my $ALIGNER_BOWTIE2 = qr{^bowtie2}msx;

# Regex for matching to reference sequence paths in HTS file header PG
# records.
#
# Do not be tempted to use npg_tracking::data::reference::find to
# recognise reference name strings. The results returned by that
# method can vary dynamically (e.g. by switching the returned path)
# and are not a canonical reference.
#
# The best we can do is pattern match for a string that has a form
# matching the values that the reference might take.
our $DEFAULT_REFERENCE_REGEX = qr{/references/}mxs;

=head2 get_records

  Arg [1]      Entire header. This may be provided as a Str or ArrayRef[Str]
               where each header row is an element of the array.
  Arg [2]      Header tag whose records are to be returned, Str.

  Example    : my @pg_records = $obj->get_records($header, 'PG');
  Description: Return an array of records for the given tag.
  Returntype : Array[Str]

=cut

sub get_records {
  my ($self, $header, $tag) = @_;

  defined $header or $self->logconfess('A defined header argument is required');

  my @records;
  if (not ref $header) {
    @records = split m{\r\n?|\n}msx, $header;
  }
  elsif (ref $header eq 'ARRAY') {
    @records = @{$header};
  }
  else {
    $self->logconfess('The header argument must be a scalar or an ArrayRef: ',
                      $header);
  }

  if (defined $tag) {
    @records = grep { m{^[@]$tag}msx } @records;
  }

  return @records;
}

=head2 get_tag_values

  Arg [1]      Header record, Str.
  Arg [2]      Header tag whose tag-values are to be returned, Str.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my @tag_values = $obj->get_tag_values($pg_record, 'CL');
               }
  Description: Return an array of tag-values for the given tag.
  Returntype : Array[Str]

=cut

sub get_tag_values {
  my ($self, $header_record, $tag) = @_;

  defined $header_record or
    $self->logconfess('A defined header_record argument is required');

  my @tag_values = split m{\t}msx, $header_record;
  if (defined $tag) {
    @tag_values = grep { m{^$tag:}msx } @tag_values;
  }

  return @tag_values;
}

=head2 get_values

  Arg [1]      Header record, Str.
  Arg [2]      Header tag whose values are to be returned, Str.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my @values = $obj->get_values($pg_record, 'CL');
               }
  Description: Return an array of values for the given tag.
  Returntype : Array[Str]

=cut

sub get_values {
  my ($self, $header_record, $tag) = @_;

  defined $header_record or
    $self->logconfess('A defined header_record argument is required');
  defined $tag or $self->logconfess('A defined tag argument is required');

  my @values;
  foreach my $tag_value ($self->get_tag_values($header_record, $tag)) {
    my ($value) = $tag_value =~ m{^$tag:(.*)$}msx;
    if (not defined $value or not length $value) {
      $self->logconfess('Failed to parse a value from tag:value pair ',
                        "'$tag_value'");
    }
    push @values, $value;
  }

  return @values;
}

=head2 get_unique_value

  Arg [1]      Header record, Str.
  Arg [2]      Header tag whose value is to be returned, Str.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my $id = $obj->get_unqiue_value($pg_record, 'ID');
               }
  Description: Return a value for the given tag or raise an error if
               there are multiple such tags. If the tag does not exist,
               return undef.
  Returntype : Str

=cut

sub get_unique_value {
  my ($self, $header_record, $tag) = @_;

  my @values = $self->get_values($header_record, $tag);
  if (scalar @values > 1) {
    $self->logconfess("Multiple '$tag' tags in '$header_record'");
  }

  my $value = shift @values;
  return $value;
}

=head2 pg_walk

  Arg [1]      Entire header. This may be provided as a Str or ArrayRef[Str]
               where each header row is an element of the array.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my $id = $obj->get_unqiue_value($pg_record, 'ID');
               }
  Description: Return walks through the PP <- PG graph(s) describing the
               programs that have acted on the data, in order. The nodes
               consist of PG records from the header. Multiple walks will
               be returned in the header contains multiple graphs.
  Returntype : ArrayRef[ArrayRef[Str]]

=cut

sub pg_walk {
  my ($self, $header) = @_;

  my @pg_records = $self->get_records($header, $PG);
  my %id_index;

  # This graph must resolve to one or more non-branching, non-circular
  # paths

  my %roots;   # The root nodes of the PP <- PG graphs
  my %edges;   # The edges of the PP <- PG graphs

  # Index the records by ID
  foreach my $pg_record (@pg_records) {
    my $id = $self->get_unique_value($pg_record, $ID);
    if (not defined $id) {
      $self->logcroak("Missing ID tag in '$pg_record'");
    }
    if (exists $id_index{$id}) {
      $self->logcroak("Duplicate ID '$id' in '$pg_record'");
    }
    else {
      $id_index{$id} = $pg_record;
    }
  }

  # Build the graph as a table of ID <- PP edges
  foreach my $pg_record (@pg_records) {
    my $pp = $self->get_unique_value($pg_record, $PP);
    my $id = $self->get_unique_value($pg_record, $ID);

    if (defined $pp) { # A program acted previously (PP) to this PG
      if (not exists $id_index{$pp}) {
        $self->logcroak("PP '$pp' in '$pg_record' without a corresponding ",
                        'PG in the header. PG graph: ', pp(\%id_index));
      }

      if (exists $edges{$pp}) {
        $self->logcroak("PP '$pp' already has child '", $edges{$pp},
                        "' when ID '$id'");
      }
      else {
        $edges{$pp} = $id;
      }
    }
    else {
      # Start of walk
      if (exists $roots{$id}) {
        $self->logcroak(q[Multiple PP walk starting points '], $roots{$id},
                        qq[' and '$id']);
      }
      else {
        $roots{$id} = 1;
      }
    }
  }

  $self->debug("Created a $PP <- $PG graph ", pp(\%edges));

  # Walk the graph, collecting the PG line for each node
  my @pg_walks;
  my @id_walks; # For debug, easier to read than whole PG lines

  foreach my $root (sort keys %roots) {
    my @pg_walk;
    my @id_walk;

    my $id = $root;
    while (defined $id) {
      push @pg_walk, $id_index{$id};
      push @id_walk, $id;
      $id = $edges{$id};
    }

    push @pg_walks, \@pg_walk;
    push @id_walks, \@id_walk;
  }

  $self->debug('Created ID walks ', pp(\@id_walks));

  return @pg_walks;
}

=head2 alignment_reference

  Arg [1]      Entire header. This may be provided as a Str or ArrayRef[Str]
               where each header row is an element of the array.
  Arg [2]      A callback to be executed on each @PG line of the BAM/CRAM
               header, CodeRef. Optional, defaults to a filter that matches on
               the default reference regex, CodeRef.

  Example    : my $reference = $obj->alignment_reference($header, sub {
                  return $_[0] =~ /my_reference/
               })
  Description: Return the reference path used in alignment. The reference
               path is parsed from the last aligner @PG line in the header
               by a simple split on whitespace, followed by application of
               the filter.

               This will give incorrect results if the reference path
               contains whitespace.

  Returntype : Str

=cut

sub alignment_reference {
  my ($self, $header, $filter) = @_;

  defined $header or
    $self->logconfess('A defined header argument is required');
  defined $filter and ref $filter ne 'CODE' and
    $self->logconfess('The filter argument must be a CodeRef');

  # The default filter for finding reference paths in command lines
  $filter ||= sub {
    return $_[0] =~ m{$DEFAULT_REFERENCE_REGEX}msx;
  };

  my $aligner_record;

  try {
    # Best option; walk the PP <- PG graph to recreate the order the
    # programs were run
    $aligner_record = $self->_find_aligner_record($self->pg_walk($header));
  } catch {
    # Fallback option; use the order of the records in the header
    $self->warn('Falling back on naive header order to find aligner reference');
    $aligner_record =
      $self->_find_aligner_record([$self->get_records($header, $PG)]);
  };

  my $reference;
  if ($aligner_record) {
    $reference = $self->_parse_pg_reference_path($aligner_record, $filter);
  }

  return $reference;
}

=head2 dehumanising_method

  Arg [1]      Entire header. This may be provided as a Str or ArrayRef[Str]
               where each header row is an element of the array.
  Arg [2]      An optional boolean flag. True value forces the return value of
               C<unknown> for split-out human data and C<see_human> for target
               data where otherwise an undefined value would have been returned.

  Example    : my $dh_method = $obj->dehumanising_method($header);
               $dh_method = $obj->dehumanising_method($header, 1);

  Description: This method returns a string value which can be used when setting
               L<iRODS dehumanised metadata|https://github.com/wtsi-npg/irods-metadata/blob/master/irods_sample_metadata.md>.
               If this method returns an undefined value, setting iRODS
               C<dehumanised> metadata is not appropriate.

               Evaluates the content of the CRAM/BAM file header to establish
               whether a procedure of removing human reads (dehumanising) has
               been performed. If the history of dehumanising is established,
               for target data and split out human data returns a string. For
               any other data returns an undefined value (for exceptions see
               the second argument description).

               Since at the time of writing (March 2025) the target file header
               header does not contain any hints about the method used to
               dehumanise (dh) the data, the value of C<see_human> is returned.
               For split-out human data a short descripton of the method is
               returned, falling back on C<unknown> when the method cannot be
               inferred.
               
               An attempt to evaluate whether the adapter clipping took place
               is undertaken. Depending on the dh method suffix C<nc> or
               C<c> might be addedto the method. Example: npg2018nc.

  Returntype : Str | undefined value

=cut

sub dehumanising_method {
  my ($self, $header, $is_dehumanised) = @_;

  my $dh_re_since2018 = qr{ bambi[ ]select
                            .+
                            alignment_filter[:]human
                          }msx;
  # In the reg. expression below 't' is optional because we have typos
  # in the headers of very old files.
  ##no critic (RegularExpressions::ProhibitComplexRegexes)
  my $dh_re_pre2018 = qr{ ID[:]AlignmentFilt?er
                          .+
                          (?:(?:HUMAN_SPLIT_BAM_OUT)|(?:_human[.]bam))
                        }msx;

  # A different type of split of data (by chromosome) should not come under
  # the 'dehumanising' umbrella.
  my $chromosome_split_re = qr{ (?:bambi[ ]chrsplit) |
                                (?:ID[:]SplitBamByChromosomes)
                              }mxs;
  ## use critic

  my @pg_lines = $self->get_records($header, $PG);

  (any { m{$chromosome_split_re}xms } @pg_lines) and return;

  my $dh_since2018_found = any { m{$dh_re_since2018}xms } @pg_lines;
  my $dh_pre2018_found = $dh_since2018_found ? undef :
    any { m{$dh_re_pre2018}xms } @pg_lines;

  # Return if no trace of dh, neither have to force it.
  ($dh_since2018_found || $dh_pre2018_found || $is_dehumanised) or return;

  my @sq_lines = $self->get_records($header, 'SQ');

  (any { m{phix}imsx } @sq_lines) and return; # This is PhiX split-out data.

  # Is this human split-out data?
  if (any { m{ /Homo_sapiens/ | SP[:]Human }msx } @sq_lines) { # Yes

    #######
    # Figure out first whether clipping took place. Looking for the record
    # of finding the adapters, then clipping them. Any of these records on
    # their own or in the wrong order do not count.
    my $clipped = 0;
    my $i_find = first_index { m{ID[:]bamadapterfind}msx } @pg_lines;
    if ($i_find >= 0 ) {
      if ($i_find < first_index { m{ID[:]bamadapterclip}msx } @pg_lines) {
        $clipped = 1; # Adapters clipped.
      }
    }

    my $method = 'unknown'; # Fallback value
    if ($dh_since2018_found) {
      if ( any { m{bowtie2[/]T2T}msx } @pg_lines ) {
        $method = 'npg2025';
      } elsif ( any { m{bwa[ ]sam[p|s]e}xms } @pg_lines ) {
        $method = 'npg2018';
      }
    } elsif ($dh_pre2018_found) {
      $method = 'npg2010';
    }

    my $clip_suffix = q[];
    if ($method eq 'npg2025') {
      if ($clipped) {
        $clip_suffix = q[c]; # We do not expect to see this case.
      }
    } elsif ($method ne 'unknown') {
      if (!$clipped) {
        $clip_suffix = q[nc];
      }
    }

    return $method . $clip_suffix; # Combine the dh method and adapter
                                   # cliping info.
  } # End of human split-out data clause.

  return 'see_human'; # This is target data with a record of having been
                      # dehumanised.
}

# Scan each graph in reverse to find an aligner PG record in one of
# them
sub _find_aligner_record {
  my ($self, @pg_graphs) = @_;

  my $num_graphs = scalar @pg_graphs;
  $self->debug("Finding aligner records in $num_graphs graphs: ",
               pp(\@pg_graphs));

  my $is_aligner = sub {
    my ($rec) = @_;

    my $id = $self->get_unique_value($rec, $ID);
    my $cl = $self->get_unique_value($rec, $CL);

    if (defined $cl) {
      if ($id =~ m{ $ALIGNER_BWA |
                    $ALIGNER_TOPHAT |
                    $ALIGNER_STAR |
                    $ALIGNER_MINIMAP2 |
                    $ALIGNER_BOWTIE2 }msx ) {
        $self->debug("ID $id identified as an aligner in $rec");
        return 1;
      }
    }
  };

  my @aligner_records;
  foreach my $graph (@pg_graphs) {
    my @pg_records = @{$graph};

    my $aligner_record = firstval { $is_aligner->($_) } reverse @pg_records;
    if ($aligner_record) {
      push @aligner_records, $aligner_record;
    }
  }

  my $num_records = scalar @aligner_records;
  if ($num_records > 1) {
    $self->logcroak("Found $num_records candidate aligner records ",
                    "in $num_graphs PP <- PP graphs: ", pp(\@aligner_records));
  }

  my $rec = shift @aligner_records;
  return $rec;
}

# Parse a reference path from an aligner PG record
sub _parse_pg_reference_path {
  my ($self, $pg_record, $filter) = @_;

  my @fields;
  foreach my $cmd_line ($self->get_values($pg_record, $CL)) {
    push @fields, split m{\s+}msx, $cmd_line;
  }

  # Note the uniq filter; the correct reference may appear multiple
  # times in the PG header record's command line.
  @fields = uniq grep { $filter->($_) } @fields;

  my $num_fields = scalar @fields;
  if ($num_fields > 1) {
    $self->logcroak("Reference filter matched $num_fields elements in PG ",
                    "record '$pg_record': ", pp(\@fields));
  }

  my $field = shift @fields;
  return $field;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::HeaderParser

=head1 DESCRIPTION

A basic parser for SAM/BAM/CRAM file headers. It exposes some general
purpose header parsing methods that may be used as building blocks for
more complex queries.

It also provides some such complex queries, such as the alignment
reference and PG graphs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

Marina Gourtovaia <mg8@sanger.ac.uk> 

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2017, 2018, 2021, 2025 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
