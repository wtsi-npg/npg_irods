package WTSI::NPG::OM::BioNano::RunPublisher;

use Moose;
use namespace::autoclean;

use DateTime;
use File::Basename qw[basename];
use File::Spec::Functions;
use UUID;
use URI;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::OM::BioNano::ResultSet;

# FIXME Move/refactor WTSI::NPG::HTS::Publisher to reflect use outside of
# HTS. Maybe consolidate with WTSI::NPG::Publisher in wtsi-npg/genotyping.

our $VERSION = '';

our @BNX_SUFFIXES = qw[bnx];

with qw[WTSI::DNAP::Utilities::Loggable
        WTSI::NPG::Accountable
        WTSI::NPG::OM::BioNano::Annotator];

has 'directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   documentation => 'Path of a BioNano runfolder to be published'
);

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'resultset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::OM::BioNano::ResultSet',
   init_arg => undef,
   lazy     => 1,
   builder  => '_build_resultset',
   documentation => 'Object containing results from a BioNano runfolder'
);

has 'uuid' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   builder  => '_build_uuid',
   documentation => 'UUID generated for the publication to iRODS');


=head2 publish

  Arg [1]    : [Str] iRODS path that will be the root destination for
               publication. BioNano will be published to a subcollection,
               with a hashed path based on the md5 checksum of the
               Molecules.bnx file.
  Arg [2]    : [DateTime] Timestamp for the time of publication. Optional,
               defaults to the present time.

  Example    : $export->publish('/foo')
  Description: Publish the BioNano ResultSet to an iRODS path.
  Returntype : True

=cut

sub publish {
    my ($self, $publish_dest, $timestamp) = @_;
    # generate a hashed path for publication
    if (! file_name_is_absolute($publish_dest)) {
        $self->logcroak(q[An absolute destination path is required for ],
                        q[iRODS publication; given path was '],
                        $publish_dest, q['])
    }
    if (! defined $timestamp) {
        $timestamp = DateTime->now();
    }
    my $hash_path =
        $self->irods->hash_path($self->resultset->bnx_path,
                                $self->resultset->bnx_file->md5sum);
    $self->debug(q[Found hashed path '], $hash_path, q[' from checksum '],
                 $self->resultset->bnx_file->md5sum, q[']);
    my $leaf_collection = catdir($publish_dest, $hash_path);
    $self->debug(q[Publishing to collection '], $leaf_collection, q[']);
    # publish data to iRODS, if not already present
    my $dirname = basename($self->resultset->directory);
    my $bionano_collection = catdir($leaf_collection, $dirname);
    if ($self->irods->list_collection($bionano_collection)) {
        $self->info(q[Skipping publication of BioNano data collection '],
                $bionano_collection, q[': already exists]);
    } else {
        my $collection_meta = $self->make_collection_meta();
        my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $self->irods);
        my $bionano_published_coll = $publisher->publish(
            $self->resultset->directory,
            $leaf_collection,
            $collection_meta,
            $timestamp,
        );
        if ($bionano_published_coll ne $bionano_collection) {
            $self->logcroak(q[Expected BioNano publication destination '],
                            $bionano_collection,
                            q[' not equal to return value from Publisher '],
                            $bionano_published_coll, q[']
                        );
        } else {
            $self->debug(q[Published BioNano runfolder '],
                         $self->resultset->directory,
                         q[' to iRODS destination '],
                         $bionano_collection, q[']
                     );
        }
        my $bnx_ipath = $self->_apply_bnx_file_metadata($bionano_collection);
        $self->debug(q[Applied metadata to BNX iRODS object '],
                     $bnx_ipath, q[']);
    }
    return $bionano_collection;
}


=head2 make_collection_meta

  Args       : None
  Example    : $collection_meta = $publisher->get_collection_meta();
  Description: Generate metadata to be applied to a BioNano collection
               in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub make_collection_meta {
    my ($self) = @_;
    my @metadata;
    # creation metadata is added by HTS::Publisher
    my @bnx_meta = $self->make_bnx_metadata($self->resultset);
    my @uuid_meta = $self->make_uuid_metadata($self->uuid);
    push @metadata, @bnx_meta, @uuid_meta;
    # TODO add sample metadata from Sequencescape (or mock DB for tests)
    return \@metadata;
}


sub _apply_bnx_file_metadata {
    my ($self, $bionano_collection) = @_;
    # apply metadata to filtered BNX file. Start with metadata applied to
    # the collection (including by HTS::Publisher)
    my @bnx_meta;
    my $md5 = $self->resultset->bnx_file->md5sum;
    push @bnx_meta, $self->irods->get_collection_meta($bionano_collection);
    push @bnx_meta, $self->make_md5_metadata($md5);
    push @bnx_meta, $self->make_type_metadata($self->resultset->bnx_path,
                                              @BNX_SUFFIXES);
    # $published_meta includes terms added by HTS::Publisher
    my $bnx_ipath = File::Spec->catfile($bionano_collection,
                                        'Detect Molecules',
                                        'Molecules.bnx');
    my $bnx_obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $bnx_ipath);
    foreach my $avu (@bnx_meta) {
        $bnx_obj->add_avu($avu->{'attribute'}, $avu->{'value'});
    }
    return $bnx_ipath;
}

sub _build_resultset {
    my ($self,) = @_;
    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $self->directory
    );
    return $resultset;
}

sub _build_uuid {
    my ($self,) = @_;
    my $uuid_bin;
    my $uuid_str;
    UUID::generate($uuid_bin);
    UUID::unparse($uuid_bin, $uuid_str);
    return $uuid_str;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::RunPublisher - An iRODS data publisher
for results from the BioNano optical mapping system.

=head1 SYNOPSIS

  my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new
    (directory => $dir);

  my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new
    (irods            => $irods_handle,
     accountee_uid    => $accountee_uid,
     affiliation_uri  => $affiliation_uri,
     resultset        => $resultset);

  # Publish to iRODS with a given timestamp
  $publisher->publish($publish_dest, $timestamp);


=head1 DESCRIPTION

This class provides methods for publishing a BioNano unit runfolder to
iRODS, with relevant metadata.

The "unit" runfolder contains data from one run on the BioNano instrument,
with a given sample, flowcell, and chip. The results of multiple runs are
typically merged together for downstream analysis and assembly.

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

=head1 DESCRIPTION

Class to publish a BioNano ResultSet to iRODS.

=cut
