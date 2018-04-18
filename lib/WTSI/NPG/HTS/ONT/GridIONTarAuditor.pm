package WTSI::NPG::HTS::ONT::GridIONTarAuditor;

use namespace::autoclean;

use Carp;
use Digest::MD5;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir catfile rel2abs];
use File::Path qw[make_path];
use File::Temp;
use IO::Uncompress::Bunzip2 qw[bunzip2 $Bunzip2Error];
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

=head2 check_all_files

  Arg [1]    : None

  Example    : my ($num_files, $num_processed, $num_errors) =
                 $obj->check_all_files
  Description: Check the destination collection for manifests and
               then audit the tar files described therein.
  Returntype : Array[Int]

=cut

sub check_all_files {
  my ($self) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my ($nf5, $np5, $ne5) = $self->check_f5_tar_files;
  my ($nfq, $npq, $neq) = $self->check_fq_tar_files;

  $num_files     += ($nf5 + $nfq);
  $num_processed += ($np5 + $npq);
  $num_errors    += ($ne5 + $neq);

  return ($num_files, $num_processed, $num_errors);
}

=head2 check_f5_tar_files

  Arg [1]    : None

  Example    : my ($num_files, $num_processed, $num_errors) =
                 $obj->check_f5_tar_files
  Description: Check the destination collection for fast5 manifests and
               then audit the tar files described therein.
  Returntype : Array[Int]

=cut

sub check_f5_tar_files {
  my ($self) = @_;

  return $self->_check_tar_files($self->f5_manifests);
}

=head2 check_fq_tar_files

  Arg [1]    : None

  Example    : my ($num_files, $num_processed, $num_errors) =
                 $obj->check_fq_tar_files
  Description: Check the destination collection for fastq manifests and
               then audit the tar files described therein.
  Returntype : Array[Int]

=cut

sub check_fq_tar_files {
  my ($self) = @_;

  return $self->_check_tar_files($self->fq_manifests);
}

sub read_manifest {
  my ($self, $manifest_path) = @_;

  my $manifest = WTSI::NPG::HTS::TarManifest->new
    (manifest_path => $manifest_path);
  $manifest->read_file;

  return $manifest;
}

sub _check_tar_files {
  my ($self, $manifests) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  if (scalar @{$manifests} == 0) {
    $self->error(sprintf q[Found no manifests in '%s'],
                 $self->dest_collection);
    $num_errors++;
  }

  foreach my $manifest (@{$manifests}) {
    foreach my $tar_path ($manifest->tar_paths) {

      my ($nf, $np, $ne) = $self->_check_tar_contents($manifest, $tar_path);
      $self->info("Checked [ $np / $nf ] files in '$tar_path' with $ne errors");
      $num_files     += $nf;
      $num_processed += $np;
      $num_errors    += $ne;
    }
  }

  return ($num_files, $num_processed, $num_errors);
}

sub _check_tar_contents {
  my ($self, $manifest, $tar_path) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my @tar_items = $manifest->tar_items($tar_path);
  $num_files = scalar @tar_items;
  $self->info("Checking $num_files files in '$tar_path'");

  my $tar_cwd;
  try {
    $self->debug("Getting '$tar_path' from iRODS");
    $tar_cwd = $self->_untar_file($tar_path);
    $self->info("Fetched '$tar_path' from iRODS");
  } catch {
    $num_errors++;
    $self->error($_);
  };

  foreach my $tar_item (@tar_items) {
    try {
      $num_processed++;
      $self->_check_checksum($tar_cwd, $tar_item);
    } catch {
      $num_errors++;
      $self->error($_);
    };
  }

  return ($num_files, $num_processed, $num_errors);
}

sub _check_checksum {
  my ($self, $tar_cwd, $tar_item) = @_;

  my $item_path     = $tar_item->item_path;
  my $abs_path      = rel2abs($item_path, $tar_cwd);
  my $item_checksum = $tar_item->checksum;

  my $bunzipped_content;
  bunzip2 $abs_path => \$bunzipped_content or
    croak "Failed to bunzip2 '$abs_path': $Bunzip2Error";

  my $checksum = Digest::MD5->new->add($bunzipped_content)->hexdigest;

  defined $checksum or
    croak "No checksum was obtained from the extracted file '$item_path'";

  if ($checksum eq $item_checksum) {
    $self->info("'$item_path' had the expected checksum '$item_checksum'");
  }
  else {
    croak "'$item_path' has checksum '$checksum' (uncompressed) where " .
      "'$item_checksum' was expected";
  }

  return;
}

# Download over a single iRODS connection using tears
sub _untar_file {
  my ($self, $tar_path) = @_;

  my $tar_cwd = catdir($self->wdir, 'download');
  make_path("$tar_cwd");

  my $tar_cmd = "npg_irods_getstream.sh '$tar_path' | tar -C '$tar_cwd' -x";
  $self->info("Executing '$tar_cmd'");

  system($tar_cmd) == 0 or
    croak("Execution of '$tar_cmd' failed: $CHILD_ERROR");

  return $tar_cwd;
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

# Read the downloaded manifests from wdir
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

# Download from iRODS to a local file in wdir
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

# Find manifests in iRODS, in the dest_collection
sub _find_manifests {
  my ($self, $type) = @_;

  my $filter = $type eq 'f5' ? qr{_fast5_manifest.txt$}msx
                             : qr{_fastq_manifest.txt$}msx;

  my ($objs, $colls) = $self->irods->list_collection($self->dest_collection);
  my @manifest_paths = sort grep { m{$filter}msx } @{$objs};

  return @manifest_paths;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONTarAuditor

=head1 DESCRIPTION

Finds the fast5 and fastq manifest files of a GridIONrun in the
nominated iRODS collection and extracts the expected tar file
locations in iRODS from the manifests. The tar files are then
downloaded and extracted to a temporary directory where their contents
are checked against the manifest. Each file listed in each manifest is
checked to ensure that its expected checksum matches the actual
checksum calculated from the extracted file.

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
