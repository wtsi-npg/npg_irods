package WTSI::NPG::OM::BioNano::Publisher;

use Moose;

use DateTime;
use File::Spec;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Collection;
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

has 'publication_time' =>
  (is       => 'ro',
   isa      => 'DateTime',
   required => 1);

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

  Arg [1]    : Str iRODS path that will be the root destination for
publication. BioNano will be published to a subcollection, with a hashed
path based on the md5 checksum of the Molecules.bnx file.

  Example    : $export->publish('/foo')
  Description: Publish the BioNano ResultSet to an iRODS path.
  Returntype : True

=cut

sub publish {
    my ($self, $publish_dest, $timestamp) = @_;
    my $bnx_path  = $self->resultset->bnx_path;
    my $md5       = $self->irods->md5sum($bnx_path);
    my $hash_path = $self->irods->hash_path($bnx_path, $md5);
    $self->debug(q{Checksum of file '}, $bnx_path,
                 q{' is '}, $md5, q{'});
    if (! File::Spec->file_name_is_absolute($publish_dest)) {
        $publish_dest = File::Spec->catdir($self->irods->working_collection,
                                           $publish_dest);
    }
    my $bionano_collection = File::Spec->catdir($publish_dest, $hash_path);
    $self->debug(q{Publishing to collection '}, $bionano_collection, q{'});

    # use low-level HTS::Publisher->publish method for directory
    # arguments: $local_path, $remote_path, $metadata, $timestamp

    my $collection_meta = $self->get_collection_meta($timestamp);

    my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $self->irods);
    $publisher->publish(
        $self->resultset->directory,
        $bionano_collection,
        $collection_meta
    );
    # TODO apply metadata to BNX files?
    return $bionano_collection;
}


=head2 get_collection_meta

  Args       : DateTime Publication time, defaults to the current time

  Example    : $collection_meta = $publisher->get_collection_meta();
  Description: Generate metadata to be applied to a BioNano collection
               in iRODS.
  Returntype : ArrayRef[HashRef] AVUs to be used as metadata

=cut

sub get_collection_meta {

    my ($self, $timestamp) = @_;

    if (not defined $timestamp) {
        $timestamp = DateTime->now;
    }

    my @metadata;


    my @creation_meta = $self->make_creation_metadata(
        $self->affiliation_uri,
        $timestamp,
        $self->accountee_uri
    );

    push @metadata, @creation_meta;

    return \@metadata;
}


=head2 publish_directory

  Arg [1]    : Str iRODS path that will be the destination for publication

  Example    : $export->publish('/foo')
  Description: Publish the directory in a BioNano::ResultSet to an
               iRODS path. Inserts a hashed directory path.
  Returntype : [Str] The newly created iRODS collection

=cut

sub publish_directory {
    my ($self, $publish_dest) = @_;
    my $bnx_path = $self->resultset->bnx_path;
    my $md5            = $self->irods->md5sum($bnx_path);
    my $hash_path      = $self->irods->hash_path($bnx_path, $md5);
    $self->debug(q{Checksum of file '}, $bnx_path,
                 q{' is '}, $md5, q{'});
    my $dest_collection = File::Spec->catdir($publish_dest, $hash_path);
    my $bionano_collection;
    if ($self->irods->list_collection($dest_collection)) {
        $self->info(q{Skipping publication of BioNano data collection '},
                    $dest_collection, q{': already exists});

        my $dir = basename($self->resultset->directory);
        $bionano_collection = File::Spec->catdir($dest_collection, $dir);
    } else {
        $self->info(q{Publishing new BioNano data collection '},
                    $dest_collection, q{'});
        $self->irods->add_collection($dest_collection);
        $bionano_collection = $self->irods->put_collection
            ($self->resultset->directory, $dest_collection);
        # TODO add metadata to the new collection ??
        # my @run_meta;
        # push @run_meta, $self->make_run_metadata(\@project_titles);
        # push @run_meta, $self->make_creation_metadata($self->affiliation_uri,
        #                                               $self->publication_time,
        #                                               $self->accountee_uri);
        # my $run_coll = WTSI::NPG::iRODS::Collection->new($self->irods,
        #                                                  $bionano_collection);
        # foreach my $m (@run_meta) {
        #     my ($attribute, $value, $units) = @$m;
        #     $run_coll->add_avu($attribute, $value, $units);
        # }
    }
    return $bionano_collection;
}

=head2 publish_files

  Arg [1]    : Str iRODS path that will be the destination for publication

  Example    : $export->publish_samples('/foo', 'S01', 'S02')
  Description: Publish the individual files within a BioNano ResultSet to
               a given iRODS path, with appropriate metadata.
  Returntype : Int number of files published

=cut

sub publish_files {
    my ($self, $publish_dest) = @_;

    defined $publish_dest or
        $self->logconfess('A defined publish_dest argument is required');

    $publish_dest eq q{} and
        $self->logconfess('A non-empty publish_dest argument is required');

    $publish_dest = File::Spec->canonpath($publish_dest);

    my $num_published = 0;

    my @files_to_publish = (
        $self->resultset->bnx_file,
        $self->resultset->raw_bnx_file,
    );
    push @files_to_publish, @{$self->resultset->ancillary_files};

    # generate metadata
    # my @bnx_metadata = $self->make_bnx_metadata($self->resultset->bnx_file);
    # my @creation_metadata;
    # my @sample_metadata

    # my $publisher = ???

    $self->debug('Ready to publish ', scalar @files_to_publish, 'files');

    foreach my $file (@files_to_publish) {
        try {
            #my @file_meta;
            #foreach my $m (@file_meta) {
            #    my ($attribute, $value, $units) = @$m;
            #    $run_coll->add_avu($attribute, $value, $units);
            #}

            $self->debug(q{Published file '}, $file, q{'});
            $num_published++;
        } catch {
            $self->error(q{Failed to publish file '}, $file, q{'});
        };
    }
    return $num_published;

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
