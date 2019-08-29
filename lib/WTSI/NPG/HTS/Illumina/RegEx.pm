package WTSI::NPG::HTS::Illumina::RegEx;

use strict;
use warnings;

our $VERSION = '';

our %ILLUMINA_REGEX_PATTERNS =
(interop_regex   => q[[.]bin],
 xml_regex       => q[(RunInfo|[rR]unParameters).xml],
 alignment_regex => q[[.](bam|cram)],
 index_regex     => q[[.](bai|cram[.]crai|pbi|vcf[.]gz[.]tbi|g[.]vcf[.]gz[.]tbi)],
 genotype_regex  => q[[.](bcf|vcf|vcf[.]gz|g[.]vcf[.]gz|geno)],
 ancillary_regex => (join q[|],
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
  '[.]txt'),
  qc_regex        => (join q[|],
  '[.]adapter[.]json',
  '[.]alignment_filter_metrics[.]json',
  '[.]bam_flagstats[.]json',
  '[.]gc_bias[.]json',
  '[.]gc_fraction[.]json',
  '[.]gc_fraction[.]json',
  '[.]genotype[.]json',
  '[.]insert_size[.]json',
  '[.]qX_yield[.]json',
  '[.]ref_match[.]json',
  '[.]samtools_stats[.]json',
  '[.]sequence_error[.]json',
  '[.]sequence_summary[.]json',
  '[.]spatial_filter[.]json',
  '[.]verify_bam_id[.]json',
  '_target[.]samtools_stats[.]json',          # non-conforming file name
  '_target_autosome[.]samtools_stats[.]json')  # non-conforming file name
);

1;

__END__

=head1 NAME
 
WTSI::NPG::HTS::Illumina::RegEx

=head1 DESCRIPTION
 
A package providing a hash of RegEx defining each group of objects
to be archived.

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
Martin Pollard <mp15@sanger.ac.uk>
 
=head1 COPYRIGHT AND DISCLAIMER
 
Copyright (C) 2018, 2019 Genome Research Limited.
All Rights Reserved.
 
This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.
 
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
 
=cut
