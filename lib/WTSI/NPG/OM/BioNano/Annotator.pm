package WTSI::NPG::OM::BioNano::Annotator;

use DateTime;
use UUID;
use Moose::Role;

use WTSI::NPG::OM::Metadata;

our $VERSION = '';

with qw[WTSI::NPG::HTS::Annotator]; # TODO better location for "parent" role

=head2 make_bnx_metadata

  Arg [1]    : WTSI::NPG::OM::BioNano::ResultSet
  Example    : @bnx_meta = $publisher->get_bnx_metadata();
  Description: Find metadata AVUs from the BNX file header, to be applied
               to a BioNano collection in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

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


=head2 make_uuid_metadata

  Arg [1]    : [Str] UUID string. Optional, defaults to generating new UUID.
  Example    : @uuid_meta = $publisher->get_uuid_metadata($uuid);
  Description: Generate a UUID metadata AVU, to be applied
               to a BioNano collection in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub make_uuid_metadata {
    my ($self, $uuid_str) = @_;
    if (! defined $uuid_str) {
        my $uuid_bin;
        UUID::generate($uuid_bin);
        UUID::unparse($uuid_bin, $uuid_str);
    }
    my @uuid_meta = (
        $self->make_avu($BIONANO_UUID, $uuid_str),
    );
    return @uuid_meta;
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
