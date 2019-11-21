package WTSI::NPG::HTS::Illumina::ResultSet;

use namespace::autoclean;
use Data::Dump qw[pp];
use Moose;
use MooseX::StrictConstructor;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'result_files' =>
  (isa           => 'ArrayRef[Str]',
   is            => 'ro',
   required      => 1,
   builder       => '_build_result_files',
   lazy          => 1,
   documentation => 'The files in the result set');

# Each key is a name by which a file-matching regex is known. Each
# value is a CodeRef which takes a product name (which may be ignored)
# and returns a regex.
our %ILLUMINA_PART_PATTERNS =
  (interop_regex   => sub {
     return q[[.]bin$];
   },
   xml_regex       => sub {
     return q[(RunInfo|[rR]unParameters).xml$];
   },
   alignment_regex => sub {
     my $name = shift;
     return sprintf q[%s[.](cram)$], "\Q$name\E";
   },
   index_regex     => sub {
     my $name = shift;
     return sprintf q[%s[.](cram[.]crai|pbi|vcf[.]gz[.]tbi|g[.]vcf[.]gz[.]tbi)$], "\Q$name\E";
   },
   genotype_regex  => sub {
     my $name = shift;
     return sprintf q[%s[.](bcf|vcf|vcf[.]gz|g[.]vcf[.]gz|geno)$], "\Q$name\E";
   },
   ancillary_regex => sub {
     my $name = shift;
     return sprintf q[(?<!qc)\/%s(_F0x[A-Z0-9]{3})?(%s)$], "\Q$name\E",
       join q[|],
       '[.]all[.]seqchksum',
       '[.]bam_stats',
       '[.]bcfstats',
       '[.]bqsr_table',
       '[.]flagstat',
       '[.]composition[.]json',
       '[.]markdups_metrics[.]txt',
       '[.]orig[.]seqchksum',
       '_quality_cycle_caltable[.]txt',   # non-conforming file name
       '_quality_cycle_surv[.]txt',       # non-conforming file name
       '_quality_error[.]txt',            # non-conforming file name
       '_salmon[.]quant[.]zip',
       '[.]seqchksum',
       '[.]sha512primesums512[.]seqchksum',
       '[.]spatial_filter[.]stats',
       '_target[.]stats',                 # non-conforming file name
       '_target_autosome[.]stats',        # non-conforming file name
       '[.]stats',
       '[.]txt';
   },
   qc_regex        => sub {
     my $name = shift;
     return sprintf q[qc\/%s(_F0x[A-Z0-9]{3})?(%s)$], "\Q$name\E",
       join q[|],
       '[.]adapter[.]json',
       '[.]alignment_filter_metrics[.]json',
       '[.]bam_flagstats[.]json',
       '[.]gc_bias[.]json',
       '[.]gc_fraction[.]json',
       '[.]gc_fraction[.]json',
       '[.]genotype[.]json',
       '[.]insert_size[.]json',
       '[.]pulldown_metrics[.]json',
       '[.]qX_yield[.]json',
       '[.]ref_match[.]json',
       '[.]samtools_stats[.]json',
       '[.]sequence_error[.]json',
       '[.]sequence_summary[.]json',
       '[.]spatial_filter[.]json',
       '[.]verify_bam_id[.]json',
       '_target[.]samtools_stats[.]json',          # non-conforming file name
       '_target_autosome[.]samtools_stats[.]json'; # non-conforming file name
   });

=head2 composition_files

  Arg [1]    : None

  Example    : $set->composition_files
  Description: Return a sorted array of composition JSON files.
  Returntype : Array

=cut

sub composition_files {
  my ($self) = @_;

  return grep { m{[.]composition[.]json$}msx } @{$self->result_files};
}

=head2 interop_files

  Arg [1]    : None

  Example    : $set->interop_files
  Description: Return a sorted array of InterOp files.
  Returntype : Array

=cut

sub interop_files {
  my ($self) = @_;

  my $regex = $self->_make_filter_regex('interop_regex');
  $self->debug("interop_regex: '$regex'");
  my $interop_subdir = 'InterOp';

  return grep { m{$interop_subdir}msx and m{$regex}msx }
    @{$self->result_files};
}

=head2 xml_files

  Arg [1]    : None

  Example    : $set->xml_files
  Description: Return a sorted array of XML files.
  Returntype : Array

=cut

sub xml_files {
  my ($self) = @_;
  my $regex = $self->_make_filter_regex('xml_regex');
  $self->debug("xml_regex: '$regex'");

  return grep { m{$regex}msx } @{$self->result_files};
}

=head2 alignment_files

  Arg [1]    : Product name, Str.

  Example    : $set->alignment_files
  Description: Return a sorted array of alignment files.
  Returntype : Array

=cut

sub alignment_files {
  my ($self, $name) = @_;

  my $regex = $self->_make_filter_regex('alignment_regex', $name);
  $self->debug("alignment_regex for $name: '$regex'");

  return grep { m{$regex}msx } @{$self->result_files};
}

=head2 index_files

  Arg [1]    : Product name, Str.

  Example    : $set->index_files
  Description: Return a sorted array of index files.
  Returntype : Array

=cut

sub index_files {
  my ($self, $name) = @_;

  return $self->_filter_files('index_regex', $name);
}

=head2 ancillary_files

  Arg [1]    : Product name, Str.

  Example    : $set->ancillary_files
  Description: Return a sorted array of ancillary files.
  Returntype : Array

=cut

sub ancillary_files {
  my ($self, $name) = @_;

  return $self->_filter_files('ancillary_regex', $name);
}

=head2 genotype_files

  Arg [1]    : Product name, Str.

  Example    : $set->genotype_files
  Description: Return a sorted array of genotype files.
  Returntype : Array

=cut

sub genotype_files {
  my ($self, $name) = @_;

  return $self->_filter_files('genotype_regex', $name);
}

=head2 genotype_files

  Arg [1]    : Product name, Str.

  Example    : $set->qc_files
  Description: Return a sorted array of QC files.
  Returntype : Array

=cut

sub qc_files {
  my ($self, $name) = @_;

  return $self->_filter_files('qc_regex', $name);
}

sub _build_result_files {
  my ($self) = @_;

  return [];
}

sub _make_filter_regex {
  my ($self, $category, $name) = @_;

  exists $ILLUMINA_PART_PATTERNS{$category} or
    $self->logconfess("Invalid file category '$category'. Expected one of : ",
                      pp([sort keys %ILLUMINA_PART_PATTERNS]));

  my $regex = $ILLUMINA_PART_PATTERNS{$category}->($name);

  return qr{$regex}msx;
}

sub _filter_files {
  my ($self, $category, $name) = @_;

  my $regex = $self->_make_filter_regex($category, $name);
  $self->debug("$category filter regex for $name: '$regex'");

  return grep { m{$regex}msx } @{$self->result_files};
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::ResultSet

=head1 DESCRIPTION

A set of Illumina result files. Files related to a product are
referred to in the API by their product "name", which is the string
used as a prefix when naming composition JSON files. i.e.

 <name>.composition.json

All files that are part of a product must share this name prefix.

Data files are divided into categories:

 - XML files; run metadata produced by the instrument.
 - InterOp files; run data produced by the instrument.
 - alignment files; the sequencing reads in BAM or CRAM format.
 - alignment index files; indices in the relevant format
 - ancillary files; files containing information about the run
 - genotype files; files with genotype calls from sequenced reads.
 - QC JSON files; JSON files containing information about the run.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
