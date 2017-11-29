package WTSI::NPG::HTS::ONT::GridIONRun;

use namespace::autoclean;

use Data::Dump qw[pp];
use File::Find;
use File::Spec::Functions qw[catfile];
use Moose;
use MooseX::StrictConstructor;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'gridion_name' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_gridion_name',
   documentation => 'The GridION instrument name (same as hostname)');

has 'experiment_name' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_experiment_name',
   documentation => 'The experiment name provided by LIMS');

has 'device_id' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_device_id',
   documentation => 'The GridION device identifier for this run');

has 'output_dir' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_output_dir',
   documentation => 'A directory path under which publisher logs and ' .
                    'manifests will be written');

has 'source_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A directory path under which sequencing result files ' .
                    'are located');

sub list_f5_files {
  my ($self) = @_;

  return $self->_find_local_files($self->source_dir, 'fast5');
}

sub list_fq_files {
  my ($self) = @_;

  return $self->_find_local_files($self->source_dir, 'fastq');
}

sub list_f5_manifest_files {
  my ($self) = @_;

  my @files = grep { m{fast5_manifest.txt$}msx }
    @{$self->list_manifest_files};

  return \@files;
}

sub list_fq_manifest_files {
  my ($self) = @_;

  my @files = grep { m{fastq_manifest.txt$}msx }
    @{$self->list_manifest_files};

  return \@files;
}

sub list_manifest_files {
  my ($self) = @_;

  my @files;
  if ($self->has_output_dir) {
    push @files, grep { m{fast[5q]_manifest[.]txt$}msx }
      @{$self->_find_local_files($self->output_dir, 'txt')};
  }

  return \@files;
}

sub list_seq_summary_files {
  my ($self) = @_;

  my @files = grep { m{sequencing_summary_\d+[.]txt$}msx }
    @{$self->_find_local_files($self->source_dir, 'txt')};

  return \@files;
}

sub list_seq_cfg_files {
  my ($self) = @_;

  return $self->_find_local_files($self->source_dir, 'cfg');
}

sub manifest_file_path {
  my ($self, $format) = @_;

  $format or
    $self->logconfess('A non-empty format argument is required');

  # The manifest file has the same name across all sessions of
  # publishing device's output. This means that repeated sessions will
  # incrementally publish the results of the device, or do nothing if
  # it is already completely published.
  return catfile($self->output_dir,
                 sprintf '%s_%s_%s_manifest.txt',
                 $self->experiment_name, $self->device_id, $format);
}

sub _find_local_files {
  my ($self, $dir, $format) = @_;

  $self->info("Finding any '$format' files under '$dir', recursively");
  my @files;

  my $regex = qr{[.]([^.]+$)}msx;
  find(sub {
         my ($f) = m{$regex}msx;
         if ($f and $f eq $format) {
           push @files, $File::Find::name
         }
       },
       $dir);
  @files = sort @files;

  $self->debug('Found ', pp(\@files));

  return \@files;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRun

=head1 DESCRIPTION


=head1 BUGS


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
