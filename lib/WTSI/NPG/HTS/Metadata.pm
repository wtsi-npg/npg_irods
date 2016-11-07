package WTSI::NPG::HTS::Metadata;

# Contains metadata constants for HTS publication
# FIXME -- move to WTSI::NPG::iRODS::Metadata

use strict;
use warnings;
use Exporter qw[import];

## no critic (Modules::ProhibitAutomaticExportation)
our @EXPORT = qw[
                 $LIBRARY_TYPE
                 $SEQCHKSUM
                 $HUMAN
                 $XAHUMAN
                 $YHUMAN
            ];

## use critic

our $VERSION = '';

# Annotation

our $LIBRARY_TYPE = 'library_type';
our $SEQCHKSUM = 'seqchksum';

# Sequence alignment filters

our $HUMAN   = 'human';   # FIXME
our $XAHUMAN = 'xahuman'; # FIXME
our $YHUMAN  = 'yhuman';  # FIXME

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Metadata

=head1 DESCRIPTION

This package exports "constants" for describing metadata.

It serves the same purpose as WTSI::NPG::iRODS::Metadata in the
perl-dnap-utilities repository, and should be merged into that package.
Once a new version of WTSI::NPG::iRODS::Metadata is released with the
constants from WTSI::NPG::HTS::Metadata, this package can be removed.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

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
