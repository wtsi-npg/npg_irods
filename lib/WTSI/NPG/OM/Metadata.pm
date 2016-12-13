package WTSI::NPG::OM::Metadata;

# Contains metadata constants for OM publication
# FIXME -- move to WTSI::NPG::iRODS::Metadata

use strict;
use warnings;
use Exporter qw[import];

## no critic (Modules::ProhibitAutomaticExportation)
our @EXPORT = qw[
                 $BIONANO_CHIP_ID
                 $BIONANO_FLOWCELL
                 $BIONANO_INSTRUMENT
                 $BIONANO_UUID
            ];

## use critic

our $VERSION = '';

# BioNano metadata

our $BIONANO_CHIP_ID    = 'bnx_chip_id';
our $BIONANO_FLOWCELL   = 'bnx_flowcell';
our $BIONANO_INSTRUMENT = 'bnx_instrument';
our $BIONANO_UUID       = 'bnx_uuid';

1;

__END__

=head1 NAME

WTSI::NPG::OM::Metadata

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
