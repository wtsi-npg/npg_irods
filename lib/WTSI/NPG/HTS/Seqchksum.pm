package WTSI::NPG::HTS::Seqchksum;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use List::AllUtils qw[any uniq];
use Moose;
use MooseX::StrictConstructor;
use Text::CSV;
use Try::Tiny;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

our $GROUP_ALL  = 'all';
our $GROUP_PASS = 'pass';
our $B_SEQ      = 'b_seq';
our $B_SEQ_QUAL = 'b_seq_qual';
our $B_SEQ_TAGS = 'b_seq_tags(BC,FI,QT,RT,TC)';
our $COUNT      = 'count';
our $GROUP      = 'group';
our $NAME_B_SEQ = 'name_b_seq';
our $SET        = 'set';

our $PAD_STRING = q[=];

has 'file_name' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 0,
   predicate     => 'has_file_name',
   documentation => 'The seqchksum file path, if not reading from a stream');

has 'records' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   init_arg      => undef,
   writer        => '_set_records',
   documentation => 'The parsed seqchksum records');

sub BUILD {
  my ($self, $args) = @_;

  if ($args->{fh} and $args->{file_name}) {
    $self->logconfess('Both a fh and a file_name argument were passed ',
                      'to the constructor.');
  }

  if ($args->{fh}) {
    $self->_set_records($self->_parse($args->{fh}));
    delete $args->{fh};
  }
  else {
    my $file_name = $self->file_name;

    my $fh;
    try {
      open $fh, '<:encoding(utf8)', $file_name or
        $self->logconfess("Failed to open '$file_name' for reading: $ERRNO");
      $self->_set_records($self->_parse($fh));
    } finally {
      if ($fh) {
        close $fh or $self->logcarp("Failed to close '$file_name'");
      }
    };
  }

  return;
}

=head2 read_groups

  Arg [1]    : None.

  Example    : my @groups = $seqcksum->read_groups
  Description: Return an array of read groups (column 0 values),
               excluding the "all" pseudo-group (rows 0-4),
               lexically sorted.
  Returntype : Array[Str]

=cut

sub read_groups {
  my ($self) = @_;

  my @records = grep { $_->{$GROUP} and
                       $_->{$GROUP} ne $GROUP_ALL } @{$self->records};
  my @read_groups = map { $_->{$GROUP} } @records;
  @read_groups = sort { $a cmp $b } uniq @read_groups;

  return @read_groups;
}

=head2 all_group

  Arg [1]    : None.

  Example    : my $name = $seqcksum->all_group
  Description: Return the name of the "all" pseudo-group which
               represents the combined seqchksum of all the read
               groups present.
  Returntype : Str

=cut

sub all_group {
  return $GROUP_ALL;
}

=head2 all_records

  Arg [1]    : None.

  Example    : my $name = $seqcksum->all_records
  Description: Return an array of all records for the "all" pseudo-group
               (rows 0-4).
  Returntype : Array[Str]

=cut

sub all_records {
  my ($self) = @_;

  my @records = grep { $_->{$GROUP} eq $GROUP_ALL } @{$self->records};

  @records or
    $self->logconfess('Failed to find records for all read groups');

  return @records;
}

=head2 read_group_records

  Arg [1]    : Read group, Str.

  Example    : my @records = $seqcksum->read_group_records($read_group);
  Description: Return an array of seqchksum records for the specified
               read group, excluding the "all" pseudo-group. The records
               are sorted lexically by their 'set' keys. Raise an error
               on an invalid read group.
  Returntype : Array[HashRef]

=cut

sub read_group_records {
  my ($self, $read_group) = @_;

  defined $read_group or
    $self->logconfess('A defined read_group argument is required');

  my @read_groups = $self->read_groups;
  any { $read_group eq $_ } @read_groups or
    $self->logconfess("Invalid read_group argument '$read_group'. ",
                      'Valid read_groups are: ', pp(\@read_groups));

  my @records = grep { $_->{$GROUP} eq $read_group } @{$self->records};
  @records = sort { $a->{$SET} cmp $b->{$SET} } @records;

  return @records;
}

=head2 digest

  Arg [1]    : Read group, Str.

  Example    : my $digest = $seqcksum->digest($read_group);
  Description: Return a digest string for the specified read group.
               Comparision of digests is sufficient to identify data
               that are semantically equivalent with respect to the
               values measured by the seqchksum. Raise an error on
               an invalid read group. Also accept the "all"
               pseudo-group and return the combined seqchksum of all
               read groups present.
  Returntype : Str

=cut

sub digest {
  my ($self, $read_group) = @_;

  my @records;
  if ($read_group eq $GROUP_ALL) {
    @records = ($self->all_records);
  }
  else {
    @records = $self->read_group_records($read_group);
  }

  return $self->_make_digest(@records);
}

sub _make_digest {
  my ($self, @read_group_records) = @_;

  # Defines order of checksums in digest
  my @digest_keys = ($B_SEQ, $NAME_B_SEQ, $B_SEQ_QUAL, $B_SEQ_TAGS);

  my @digest_fields;
  foreach my $record (@read_group_records) {
    push @digest_fields,
      map { sprintf '%08x', hex $record->{$_} } @digest_keys;
  }

  my $digest = join $PAD_STRING, @digest_fields;
  # Fill pad spaces with the pad string
  $digest =~ s/\s/$PAD_STRING/msx;

  return $digest;
}

sub _parse {
  my ($self, $fh) = @_;

  defined $fh or $self->logconfess('A defined fh argument is required');

  my $tsv = Text::CSV->new
    ({binary           => 1,
      eol              => "\n",
      sep_char         => "\t",
      allow_whitespace => 0,
      quote_char       => undef}) or
        $self->logconfess('Failed to create a TSV parser: ',
                          Text::CSV->error_diag);

  my $header = $tsv->getline($fh);
  if (not $header) {
    $self->logconfess('Failed to parse the seqchksum header');
  }

  my @column_names = @{$header};
  $column_names[0] = $GROUP; # Replace '###' with 'group'
  $tsv->column_names(@column_names);

  my @records;
  while (my $rec = $tsv->getline_hr($fh)) {
    # seqchksum files have an empty column (two adjacent tabs); remove
    # this
    delete $rec->{q[]};
    push @records, $rec;
  }

  return \@records;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Seqchksum

=head1 DESCRIPTION

This class provides a parser and digest-maker for the output of the
program bamseqchksum from the biobambam suite
(https://github.com/gt1/biobambam). The bamseqchksum program
calculates, for each read group within a BAM/CRAM file, checksums for
various parts of the records in those read groups. It also counts the
total number of reads and the number of reads passing QC in each read
group and in total.

The file format is tab-delimited and has a single header line. Literal
tabs are denoted by spaces below:

### set count  b_seq name_b_seq b_seq_qual b_seq_tags(BC,FI,QT,RT,TC)
all  all n  c1 c2 c3 c4
all pass m  c5 c6 c7 c8
  all 0  1 1 1 1
 pass 0  1 1 1 1
19136_8#12  all t1   c9 c10 c11 c12
19136_8#12 pass t2  c13 c14 c15 c16
19137_8#12  all t3  c17 c18 c19 c20
19137_8#12 pass t4  c21 c22 c23 c24

Note that column 0 contains an empty string in rows 3 and 4. These
rows describe reads that do not belong to a read group, thereby having
no read group ID.

The column content is as follows:

Column 0,                        '###': A read group ID or the token 'all'.
Column 1,                        'set': The token 'all' or 'pass'.
Column 2,                      'count': Integer
Column 3,                           '': Empty column
Column 4,                      'b_seq': SAM bitfields & sequence checksum
Column 5,                 'name_b_seq': read name, SAM bitfield & sequence
                                        checksum
Column 6,                 'b_seq_qual': SAM bitfields, sequence and sequence
                                        qualities checksum
Column 7, 'b_seq_tags(BC,FI,QT,RT,TC)': SAM bitfields, sequence and read tags
                                        checksum (values in parentheses
                                        denoting the tags used).

N.B Not all the bits in the SAM bitfields byte are used in
checksumming; three of them are used. From the SAM spec
(http://samtools.github.io/hts-specs/)

  0x1 template having multiple segments in sequencing
  0x4 the first segment in the template
  0x8 the last segment in the template

The read set 'all' denotes results obtained from examining all reads,
while 'pass' denotes results obtained from examining only reads that
have passed QC (have the SAM bitfield QC bit set).

n = total number of reads in all read groups
m = total number of reads in all read groups, passing QC

t1, t3 = total number of reads in each read group
t2, t4 = total number of reads in each read group, passing QC

c1-c4 = checksum of reads in all read groups
c5-c8 = checksum of reads in all read groups, passing QC

c9-c12,  c17-c20 = checksum of reads in each read group
c13-c16, c21-c24 = checksum of reads in each read group, passing QC

The default checksum is crc32prod. The checksums are not left-padded
with zeroes, so some may be shorter than others.

The rows are parsed into HashRef records of the form:

  {
    'b_seq'                      => '323e8897',
    'b_seq_qual'                 => '73464b1c',
    'b_seq_tags(BC,FI,QT,RT,TC)' => '68baabcc',
    'count'                      => 71488156,
    'group'                      => 'all',
    'name_b_seq'                 => '84f885e',
    'set'                        => 'all'
  }

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
