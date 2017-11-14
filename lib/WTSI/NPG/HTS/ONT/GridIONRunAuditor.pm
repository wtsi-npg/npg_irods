package WTSI::NPG::HTS::ONT::GridIONRunAuditor;

use namespace::autoclean;

use Data::Dump qw[pp];
use File::Basename;
use File::Spec::Functions qw[abs2rel catdir catfile rel2abs splitdir];
use Moose;
use MooseX::StrictConstructor;
use Sys::Hostname;

use WTSI::NPG::HTS::ONT::GridIONRun;
use WTSI::NPG::HTS::TarManifest;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

# These methods are autodelegated to gridion_run
our @HANDLED_RUN_METHODS = qw[device_id
                              experiment_name
                              gridion_name
                              has_device_id
                              has_experiment_name
                              has_gridion_name
                              has_output_dir
                              output_dir
                              source_dir];

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'gridion_run' =>
  (isa           => 'WTSI::NPG::HTS::ONT::GridIONRun',
   is            => 'ro',
   required      => 1,
   handles       => [@HANDLED_RUN_METHODS],
   documentation => 'The GridION run');

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

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (not ref $args[0]) {
    my %args = @args;

    my $run = WTSI::NPG::HTS::ONT::GridIONRun->new
      (gridion_name => delete $args{gridion_name},
       output_dir   => delete $args{output_dir},
       source_dir   => delete $args{source_dir});

    return $class->$orig(gridion_run => $run, %args);
  }
  else {
    return $class->$orig(@args);
  }
};

sub BUILD {
  my ($self) = @_;

  -e $self->source_dir or
    $self->logconfess(sprintf q[Data directory '%s' does not exist],
                      $self->source_dir);
  -d $self->source_dir or
    $self->logconfess(sprintf q[Data directory '%s' is not a directory],
                      $self->source_dir);

  my ($device_id, $ename, @rest) = reverse splitdir($self->source_dir);
  $self->experiment_name($ename);
  $self->device_id($device_id);

  return;
}

sub check_all_files {
  my ($self) = @_;

  my ($num_files, $num_present) = (0, 0);
  my @missing;

  foreach my $result ([$self->check_seq_cfg_files],
                      [$self->check_seq_summary_files],
                      [$self->check_manifest_files],
                      [$self->check_f5_tar_files],
                      [$self->check_fq_tar_files],
                      [$self->check_f5_files],
                      [$self->check_fq_files]) {
    my ($nf, $np, $m) = @{$result};
    $num_files   += $nf;
    $num_present += $np;
    push @missing, @{$m};
  }

  @missing = sort @missing;

  return ($num_files, $num_present, \@missing);
}

sub check_f5_tar_files {
  my ($self) = @_;

  return $self->_check_tar_files($self->f5_manifests);
}

sub check_fq_tar_files {
  my ($self) = @_;

  return $self->_check_tar_files($self->fq_manifests);
}

sub check_f5_files {
  my ($self) = @_;

  my $local_files = $self->gridion_run->list_f5_files;

  return $self->_check_tar_content($self->f5_manifests, $local_files);
}

sub check_fq_files {
  my ($self) = @_;

  my $local_files = $self->gridion_run->list_fq_files;

  return $self->_check_tar_content($self->fq_manifests, $local_files);
}

sub check_manifest_files {
  my ($self) = @_;

  my $paths = $self->gridion_run->list_manifest_files;

  return $self->_check_ancillary_files($paths);
}

sub check_seq_summary_files {
  my ($self) = @_;

  my $paths = $self->gridion_run->list_seq_summary_files;

  return $self->_check_ancillary_files($paths);
}

sub check_seq_cfg_files {
  my ($self) = @_;

  my $paths = $self->gridion_run->list_seq_cfg_files;

  return $self->_check_ancillary_files($paths);
}

sub read_manifest {
  my ($self, $manifest_path) = @_;

  my $manifest = WTSI::NPG::HTS::TarManifest->new
    (manifest_path => $manifest_path);
  $manifest->read_file;

  return $manifest;
}

sub _check_ancillary_files {
  my ($self, $local_paths) = @_;

  my $collection = catdir($self->dest_collection, $self->gridion_name,
                          $self->experiment_name, $self->device_id);

  my ($num_files, $num_present) = (0, 0);
  my @missing;

  foreach my $path (@{$local_paths}) {
    $self->debug("Checking for '$path' in '$collection'");
    $num_files++;

    my $filename = fileparse($path);
    my $obj = WTSI::NPG::iRODS::DataObject->new(collection  => $collection,
                                                data_object => $filename,
                                                irods       => $self->irods);
    if ($obj->is_present) {
      $num_present++;
      $self->debug("'$path' is present in iRODS")
    }
    else {
      push @missing, $path;
      $self->debug("'$path' missing from iRODS");
    }
  }

  @missing = sort @missing;

  return ($num_files, $num_present, \@missing);
}

sub _check_manifest_tar_files {
  my ($self, $manifest) = @_;

  my ($num_files, $num_present) = (0, 0);
  my @missing;

  foreach my $path ($manifest->tar_files) {
    $self->debug("Checking for '$path'");
    $num_files++;

    my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $path);
    if ($obj->is_present) {
      $num_present++;
      $self->debug("'$path' is present in iRODS")
    }
    else {
      push @missing, $path;
      $self->debug("'$path' missing from iRODS");
    }
  }

  @missing = sort @missing;

  return ($num_files, $num_present, \@missing);
}

sub _check_tar_files {
  my ($self, $manifests) = @_;

  my ($num_files, $num_present) = (0, 0);
  my @missing;

  foreach my $manifest (@{$manifests}) {
    my ($nf, $np, $m)= $self->_check_manifest_tar_files($manifest);
    $num_files   += $nf;
    $num_present += $np;
    push @missing, @{$m};
  }

  @missing = sort @missing;

  return ($num_files, $num_present, \@missing);
}

sub _check_tar_content {
  my ($self, $manifests, $local_paths) = @_;

  my @manifest_paths = map { $_->manifest_path } @{$manifests};
  $self->debug('Checking content of manifests ', pp(\@manifest_paths));

  my ($num_files, $num_present) = (0, 0);
  my @missing;

  foreach my $local_path (@{$local_paths}) {
    $num_files++;

    # tar files are created relative to the parent of the experiment
    # directory, so experiment_name and device_id must be added to the
    # path
    my $item_path = catdir($self->experiment_name,
                           $self->device_id,
                           abs2rel($local_path, $self->source_dir));
    $item_path .= '.bz2';

    $self->debug("Checking for item '$item_path'");

    my $in;
  MANIFEST: foreach my $manifest (@{$manifests}) {
      if ($manifest->contains_item($item_path)) {
        $in = $manifest;
        last MANIFEST;
      }
    }

    if ($in) {
      $num_present++;
      $self->debug("$item_path present in ", $in->manifest_path);
    }
    else {
      push @missing, $item_path;
    }
  }

  @missing = sort @missing;

  return ($num_files, $num_present, \@missing);
}

sub _build_irods {
  my ($self) = @_;

  return WTSI::NPG::iRODS->new;
}

sub _build_f5_manifests {
  my ($self) = @_;

  my @manifests;
  foreach my $path (@{$self->gridion_run->list_f5_manifest_files}) {
    push @manifests, $self->read_manifest($path);
  }

  return \@manifests;
}

sub _build_fq_manifests {
  my ($self) = @_;

  my @manifests;
  foreach my $path (@{$self->gridion_run->list_fq_manifest_files}) {
    push @manifests, $self->read_manifest($path);
  }

  return \@manifests;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRunAuditor

=head1 DESCRIPTION


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
