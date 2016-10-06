package WTSI::NPG::OM::BioNano::Publisher;

use Moose;

use DateTime;
use File::Spec;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::OM::BioNano::ResultSet;
#use WTSI::NPG::OM::BioNano::Metadata; # TODO put new metadata (if any) in here, pending addition to perl-irods-wrap

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

with 'WTSI::DNAP::Utilities::Loggable';

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

}

=head2 publish

  Arg [1]    : Str iRODS path that will be the destination for publication

  Example    : $export->publish('/foo')
  Description: Publish the BioNano ResultSet to an iRODS path.
  Returntype : True

=cut

sub publish {
    my ($self, $publish_dest) = @_;

    my $bionano_collection = $self->publish_directory($publish_dest);

    $self->debug("Publishing to collection '", $bionano_collection, "'");

    #$self->publish_files($bionano_collection);

    return 1;

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
    my $molecules_file = $self->resultset->molecules_file;
    my $md5            = $self->irods->md5sum($molecules_file);
    my $hash_path      = $self->irods->hash_path($molecules_file, $md5);
    $self->debug("Checksum of file '$molecules_file' is '$md5'");

    my $dest_collection = File::Spec->catdir($publish_dest, $hash_path);

    my $bionano_collection;
    if ($self->irods->list_collection($dest_collection)) {
        $self->info("Skipping publication of BioNano data collection ",
                    "'$dest_collection': already exists");

    my $dir = basename($self->resultset->directory);
    $bionano_collection = File::Spec->catdir($dest_collection, $dir);
    } else {
        $self->info("Publishing new BioNano data collection '",
                    $dest_collection, "'");
        $self->irods->add_collection($dest_collection);
        $bionano_collection = $self->irods->put_collection
            ($self->resultset->directory, $dest_collection);
    }
    return $bionano_collection;
}

# sub publish_files


our $VERSION = '';

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
