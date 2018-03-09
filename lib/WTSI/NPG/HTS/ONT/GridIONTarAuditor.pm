package WTSI::NPG::HTS::ONT::GridIONTarAuditor;

use namespace::autoclean;

use Carp;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catfile rel2abs];
use File::Temp;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::HTS::TarManifest;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS;


with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::ChecksumCalculator
       ];

our $VERSION = '';

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_irods',
   documentation => 'An iRODS handle to run searches and perform updates');

has 'f5_manifests' =>
  (isa           => 'ArrayRef[WTSI::NPG::HTS::TarManifest]',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_f5_manifests',
   documentation => 'The manifests describing tar files sent to iRODS');

has 'fq_manifests' =>
  (isa           => 'ArrayRef[WTSI::NPG::HTS::TarManifest]',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_fq_manifests',
   documentation => 'The manifests describing tar files sent to iRODS');

has 'tmpdir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => '/tmp',
   documentation => 'Temporary directory where wdir will be created');

has 'wdir' =>
  (isa           => 'File::Temp::Dir',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_wdir',
   clearer       => 'clear_wdir',
   lazy          => 1,
   builder       => '_build_wdir',
   documentation => 'Working directory for tar file item manipulation');

sub check_all_files {
  my ($self) = @_;

  my ($num_files, $num_present, $num_errors) = (0, 0, 0);

  # Find the manifests in iRODS

  # foreach tar file
  ## check that the tar file is present
  ## download tar file
  ## untar tar file
  ## foreach item in tar file
  ### check the md5 against the manifest

  # Check the metadata in the tar files?
}

sub check_f5_tar_files {
  my ($self) = @_;


}

sub check_fq_tar_files {
  my ($self) = @_;


}

sub _check_tar_file {
  my ($self, $manifest, $tar_path) = @_;


}

sub read_manifest {
  my ($self, $manifest_path) = @_;

  my $manifest = WTSI::NPG::HTS::TarManifest->new
    (manifest_path => $manifest_path);
  $manifest->read_file;

  return $manifest;
}

sub zombat {
  my ($self) = @_;

  foreach my $manifest (@{$self->f5_manifests}) {
    foreach my $tar_file (@{$manifest->tar_files}) {
      $self->debug("Getting '$tar_file'");

      $self->wombat($tar_file);
    }
  }
}

sub wombat {
  my ($self, $tar_path) = @_;

  my $dir = $self->wdir;

  make_path("$dir/download");
  my $get= WTSI::DNAP::Utilities::Runnable->new
    (executable => 'npg_irods_getstream.sh',
     arguments   => ["$tar_path"]);
  my $tar = WTSI::DNAP::Utilities::Runnable->new
    (executable => 'tar',
     arguments  => ['-x', '-C', "$dir/download"]);
  $get->pipe($tar);

  return $tar_path;
}

sub _build_irods {
  my ($self) = @_;

  return WTSI::NPG::iRODS->new;
}

sub _build_wdir {
  my ($self) = @_;

  return File::Temp->newdir('GridIONTarAuditor.' . $PID . '.XXXXXXXXX',
                            DIR => $self->tmpdir, CLEANUP => 1);
}


sub _build_f5_manifests {
  my ($self) = @_;

  return $self->_build_manifests('f5');
}

sub _build_fq_manifests {
  my ($self) = @_;

  return $self->_build_manifests('fq');
}

sub _build_manifests {
  my ($self, $type) = @_;

  defined $type or
    $self->logconfess('A defined type argument is required');
  ($type eq 'f5' or $type eq 'fq') or
    $self->logconfess('The type argument must be be either "f5" or "fq"');

  my @manifests = map { $self->read_manifest($_) }
    $self->_get_manifests($type);

  return \@manifests;
}

sub _get_manifests {
  my ($self, $type) = @_;

  my @file_paths;
  foreach my $manifest_path ($self->_find_manifests($type)) {
    my ($filename, $collection) = fileparse($manifest_path);
    my $file_path = catfile($self->wdir, $filename);

    $self->debug("Getting manifest '$manifest_path' from iRODS to $file_path");
    push @file_paths, $self->irods->get_object($manifest_path, $file_path);
  }

  return @file_paths;
}

sub _find_manifests {
  my ($self, $type) = @_;

  my $filter = $type eq 'f5' ? qr{_fast5_manifest.txt$}
                             : qr{_fastq_manifest.txt$};

  my ($objs, $colls) = $self->irods->list_collection($self->dest_collection);
  my @manifest_paths = sort grep { m{$filter}msx } @{$objs};

  return @manifest_paths;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
__END__

=head1 NAME



=head1 DESCRIPTION



=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
