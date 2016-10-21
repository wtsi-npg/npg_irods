package WTSI::NPG::OM::BioNano::Annotator;

use DateTime;
use Moose::Role;

our $VERSION = '';


with qw/WTSI::NPG::HTS::Annotator/;


# Based on genotyping WTSI::NPG::Annotator, and WTSI::NPG::HTS::Annotator
# TODO consolidate Annotator functionality in one place where possible


# get metadata for bionano_instrument, bionano_chip_id, bionano_flowcell
# from BioNano::Resultset, parsed from BNX file header

# TODO make a BioNano Metadata class (to be merged into perl-irods-wrap)
our $BIONANO_CHIP_ID = 'bnx_chip_id';
our $BIONANO_FLOWCELL = 'bnx_flowcell';
our $BIONANO_INSTRUMENT = 'bnx_instrument';

sub make_bnx_metadata {
    my ($self, $resultset) = @_;
    my $bnx = $resultset->bnx_file;
    my @bnx_meta = (
        $self->make_avu($BIONANO_CHIP_ID, $bnx->chip_id),
        $self->make_avu($BIONANO_FLOWCELL, $bnx->flowcell),
        $self->make_avu($BIONANO_INSTRUMENT, $bnx->instrument),
    );
    return @bnx_meta;
}


no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Annotator

=head1 DESCRIPTION

A role providing methods to generate metadata for WTSI Optical Mapping
runs.

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
