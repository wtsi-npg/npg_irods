package WTSI::NPG::HTS::Illumina::FilenameParser;

use File::Basename;
use Moose::Role;

our $VERSION = '';

=head2 parse_file_name

  Arg [1]      Full path or file name of a file named using
               the NPG convention for run/lane/plex data

  Example    : my @pg_records = $obj->get_records($header, 'PG');
  Description: Return an array containing run identifier, position,
               tag index, alignment filter subset and format (suffix).
               The tag index and/or alignment filter values may be
               undef. The file mustbe named using the NPG convention
               for run/lane/plex data e.g.

                  17550_1.cram
                  17550_1#0.cram
                  17550_1#0_F0x900.stats
                  17550_1#0_phix.cram
                  17550_1#0_phix_F0x900.stats

  Returntype : Array[Str]

=cut

sub parse_file_name {
  my ($self, $path) = @_;

  my ($file_name, $directories, $suffix) = fileparse($path);

  # FIXME -- use controlled vocabulary
  my $splits = qr{(human|nonhuman|xahuman|yhuman|phix)}msx;

  my ($id_run, $position,
      $old_align_filter1, $old_align_filter2,
      $tag_index1,        $tag_index2,
      $align_filter1,     $align_filter2,
      $ancillary, $format) =
        $file_name =~ m{
                         (\d+)             # Run ID
                         _                 # Separator
                         (\d)              # Position
                         (_$splits)?       # Old alignment filter
                         (\#(\d+))?        # Tag index
                         (_$splits)?       # Alignment filter
                         (\S+)?            # Ancillary
                         [.][^.]+$         # File format
                     }mxs;

  my $tag_index    = $tag_index2;
  my $alignment_filter = $old_align_filter2 || $align_filter2;

  return ($id_run, $position, $tag_index, $alignment_filter, $format);
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::FilenameParser

=head1 DESCRIPTION

This role is to be used where it is necessary to parse the names of
Illumina sequencing data files produced by NPG. The file names are
structured such that their run identifier, lane position, plex (if
any) and alignment filters (if any) may be determined.

This should be considered a legacy or last resort method of
determining this information. Future work should use the
npg_tracking::glossary::composition package to determine the
provenance of data in sequencing results.


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
