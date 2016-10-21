package WTSI::NPG::OM::BioNano::Publisher;

use Moose;

use DateTime;
use File::Spec;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::OM::BioNano::ResultSet;
#use WTSI::NPG::OM::BioNano::Metadata; # TODO put new metadata (if any) in here, pending addition to perl-irods-wrap


# FIXME Move/refactor WTSI::NPG::HTS::Publisher to reflect use outside of
# HTS. Maybe consolidate with WTSI::NPG::Publisher in wtsi-npg/genotyping.

# TODO composition with HTS::Publisher for low-level functionality?

# TODO use $irods->hash_path to create a hashed path on iRODS

# TODO use $irods->put_collection to upload the runfolder

# TODO assign a uuid to the runfolder?

# TODO do we publish/track entire runfolders, or just BNX files?

# TODO This class must:
# - upload unit runfolders to irods
# - assign metadata
# - validate checksums?
# - cross-reference with Sequencescape
#   - cf. update_secondary_metadata in Fluidigm::AssayDataObject
#   - apply eg. sanger_sample_id, internal_id, study_id

our $VERSION = '';

with qw/WTSI::DNAP::Utilities::Loggable
        WTSI::NPG::Accountable
        WTSI::NPG::OM::BioNano::Annotator/;

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

#has 'publication_time' =>
#  (is       => 'ro',
#   isa      => 'DateTime',
#   required => 1);

has 'resultset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::OM::BioNano::ResultSet',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);

  return 1;
}

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
    my $bnx_path  = $self->resultset->bnx_path;
    my $md5       = $self->irods->md5sum($bnx_path);
    my $hash_path = $self->irods->hash_path($bnx_path, $md5);
    if (! defined $timestamp) {
        $timestamp = DateTime->now();
    }
    $self->debug(q{Checksum of file '}, $bnx_path,
                 q{' is '}, $md5, q{'});
    if (! File::Spec->file_name_is_absolute($publish_dest)) {
        $publish_dest = File::Spec->catdir($self->irods->working_collection,
                                           $publish_dest);
    }
    my $leaf_collection = File::Spec->catdir($publish_dest, $hash_path);
    $self->debug(q{Publishing to collection '}, $leaf_collection, q{'});

    # use low-level HTS::Publisher->publish method for directory
    # arguments: $local_path, $remote_path, $metadata, $timestamp

    # redundant assignment of dcterms:created in HTS::Publisher and
    # HTS::Annotator. Using the same timestamp argument ensures consistency.

    my $collection_meta = $self->make_collection_meta($timestamp);

    my $publisher = WTSI::NPG::HTS::Publisher->new(
        irods => $self->irods,
    );
    my $bionano_collection = $publisher->publish(
        $self->resultset->directory,
        $leaf_collection,
        $collection_meta,
        $timestamp,
    );
    # apply metadata to filtered BNX file
    my @published_meta =
        $self->irods->get_collection_meta($bionano_collection);
    # $published_meta includes terms added by HTS::Publisher
    my $bnx_ipath = File::Spec->catfile($bionano_collection,
                                        'Detect Molecules',
                                        'Molecules.bnx');
    my $bnx_obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $bnx_ipath);
    foreach my $avu (@published_meta) {
        $bnx_obj->add_avu($avu->{'attribute'}, $avu->{'value'});
    }
    $bnx_obj->add_avu('type', 'bnx');
    $bnx_obj->add_avu($FILE_MD5, $md5);
    return $bionano_collection;
}


=head2 make_collection_meta

  Arg [1]    : [DateTime] Publication time, defaults to the current time

  Example    : $collection_meta = $publisher->get_collection_meta();
  Description: Generate metadata to be applied to a BioNano collection
               in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub make_collection_meta {

    my ($self, $timestamp) = @_;

    if (not defined $timestamp) {
        $timestamp = DateTime->now;
    }

    my @metadata;
    # creation metadata is added by HTS::Publisher
    my @bnx_meta = $self->make_bnx_metadata($self->resultset);
    push @metadata, @bnx_meta;
    # TODO add sample metadata from mock Sequencescape instance

    return \@metadata;
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

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
