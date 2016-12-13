package WTSI::NPG::OM::BioNano::Annotator;

use DateTime;
use Moose::Role;
use UUID;

use WTSI::NPG::OM::Metadata;

our $VERSION = '';

our $STOCK_IDENTIFIER  = 'stock_id';
our $SOURCE            = 'source';
our $PRODUCTION_SOURCE = 'production';

with qw[WTSI::NPG::iRODS::Annotator];

has 'uuid' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   builder  => '_build_uuid',
   documentation => 'UUID generated for the publication to iRODS');

=head2 make_bnx_metadata

  Arg [1]    : WTSI::NPG::OM::BioNano::ResultSet
  Example    : @bnx_meta = $publisher->get_bnx_metadata();
  Description: Find metadata AVUs from the BNX file header, to be applied
               to a BioNano collection in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub make_bnx_metadata {
    my ($self, $bnx) = @_;
    my @avus = (
        $self->make_avu($BIONANO_CHIP_ID, $bnx->chip_id),
        $self->make_avu($BIONANO_FLOWCELL, $bnx->flowcell),
        $self->make_avu($BIONANO_INSTRUMENT, $bnx->instrument),
    );
    return @avus;
}

=head2 make_collection_metadata

  Arg [1]    : WTSI::NPG::OM::BioNano::ResultSet. Required.
  Arg [2]    : Array[WTSI::DNAP::Warehouse::Schema::Result::StockResource]
               ML warehouse Stock records
  Example    : $coll_meta = $publisher->make_collection_metadata($rs, @stock);
  Description: Generate metadata to be applied to a BioNano collection
               in iRODS.
  Returntype : Array[HashRef] AVUs to be used as metadata

=cut

sub make_collection_metadata {
    my ($self, $resultset, @stock_records) = @_;
    my @avus;
    if (! defined $resultset) {
        $self->logcroak('BioNano::ResultSet argument is required');
    }
    if (scalar @stock_records == 0) {
        $self->logwarn('StockResource argument is empty; no sample/study ',
                       'metadata will be added');
    }
    # creation metadata is added by HTS::Publisher
    my @primary_meta = $self->make_primary_metadata(
        $resultset->bnx_file,
    );
    my @secondary_meta = $self->make_secondary_metadata(
        $resultset->stock,
        @stock_records,
    );
    push @avus, @primary_meta, @secondary_meta;
    return @avus;
}

=head2 make_primary_metadata

  Arg [1]    : WTSI::NPG::OM::BioNano::BnxFile. Required.
  Example    : @primary_meta = $publisher->make_primary_metadata($bnx);
  Description: Generate primary metadata AVUs, to be applied
               to a BioNano collection in iRODS.
  Returntype : Array[HashRef] AVUs to be used as metadata

=cut

sub make_primary_metadata {
    my ($self, $bnx) = @_;
    if (! defined $bnx) {
        $self->logcroak('BnxFile argument is required');
    }
    my @avus;
    push @avus, $self->make_bnx_metadata($bnx);
    push @avus, $self->make_uuid_metadata($self->uuid);
    return @avus;
}


=head2 make_secondary_metadata

  Arg [1]    : Str. Stock UUID parsed from the BioNano runfolder name.
               Required.
  Arg [2]    : Array[WTSI::DNAP::Warehouse::Schema::Result::StockResource]
               ML warehouse Stock records
  Example    : @secondary_meta = $p->make_secondary_metadata(@stock);
  Description: Generate secondary metadata AVUs, including sample and
               study information from the ML Warehouse database, to be
               applied to a BioNano collection in iRODS.
  Returntype : Array[HashRef] AVUs to be used as metadata

=cut

sub make_secondary_metadata {
    my ($self, $stock_id, @stock_records) = @_;
    if (! defined $stock_id ) {
        $self->logcroak('Stock ID argument is required');
    }
    my @avus;
    push @avus, $self->make_avu($SOURCE, $PRODUCTION_SOURCE);
    push @avus, $self->make_avu($STOCK_IDENTIFIER, $stock_id);
    my @samples = map { $_->sample } @stock_records;
    my @studies = map { $_->study } @stock_records;
    push @avus, $self->make_sample_metadata(@samples);
    push @avus, $self->make_study_metadata(@studies);
    return @avus;
}


=head2 make_uuid_metadata

  Arg [1]    : None
  Example    : @uuid_meta = $publisher->get_uuid_metadata();
  Description: Generate a UUID metadata AVU, to be applied
               to a BioNano collection in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub make_uuid_metadata {
    my ($self) = @_;
    my @avus = (
        $self->make_avu($BIONANO_UUID, $self->uuid),
    );
    return @avus;
}


sub _build_uuid {
    my ($self,) = @_;
    my $uuid_bin;
    my $uuid_str;
    UUID::generate($uuid_bin);
    UUID::unparse($uuid_bin, $uuid_str);
    return $uuid_str;
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
