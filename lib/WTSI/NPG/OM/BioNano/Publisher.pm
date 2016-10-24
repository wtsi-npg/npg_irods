package WTSI::NPG::OM::BioNano::Publisher;

use Moose;

use DateTime;
use File::Basename qw(basename);
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

our @BNX_SUFFIXES = qw[bnx];

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
    my $hash_path =
        $self->irods->hash_path($self->resultset->bnx_path,
                                $self->resultset->bnx_file->md5sum);
    $self->debug(q{Found hashed path '}, $hash_path, q{' from checksum '},
                 $self->resultset->bnx_file->md5sum, q{'});
    if (! defined $timestamp) {
        $timestamp = DateTime->now();
    }
    if (! File::Spec->file_name_is_absolute($publish_dest)) {
        $publish_dest = File::Spec->catdir($self->irods->working_collection,
                                           $publish_dest);
    }
    my $leaf_collection = File::Spec->catdir($publish_dest, $hash_path);
    $self->debug(q{Publishing to collection '}, $leaf_collection, q{'});

    # TODO need a 'fingerprint' or UUID for the runfolder

    my $dirname = basename($self->resultset->directory);
    my $bionano_collection = File::Spec->catdir($leaf_collection, $dirname);
    if ($self->irods->list_collection($bionano_collection)) {
        $self->info(q{Skipping publication of BioNano data collection '},
                $bionano_collection, q{': already exists});
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
            $self->logcroak(q{Expected BioNano publication destination '},
                            $bionano_collection,
                            q{' not equal to return value from Publisher '},
                            $bionano_published_coll, q{'}
                        );
        } else {
            $self->debug(q{Published BioNano runfolder '},
                         $self->resultset->directory,
                         q{' to iRODS destination '},
                         $bionano_collection, q{'}
                     );
        }
        my $bnx_ipath = $self->_apply_bnx_metadata($bionano_collection);
        $self->debug(q{Applied metadata to BNX iRODS object '},
                     $bnx_ipath, q{'});
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
    push @metadata, @bnx_meta;
    # TODO add sample metadata from Sequencescape (or mock DB for tests)

    return \@metadata;
}


sub _apply_bnx_metadata {
    my ($self, $bionano_collection) = @_;
    # apply metadata to filtered BNX file
    # start with metadata applied to the collection
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
