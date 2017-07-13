package WTSI::NPG::HTS::TarPublisher;

use namespace::autoclean;

use English qw[-no_match_vars];
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::HTS::TarStream;

our $VERSION= '';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

has 'manifest_index' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'Maps each file path input to publish_file to the tar ' .
                    'file storing that file.');

has 'manifest_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A manifest of published read files');

has 'tar_capacity' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 10_000,
   documentation => 'The maximum number of files that will be added to any ' .
                    'tar file');

has 'tar_count' =>
  (isa           => 'Int',
   is            => 'rw',
   required      => 1,
   default       => 0,
   init_arg      => undef,
   documentation => 'The number of tar files published');

has 'tar_cwd' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The CWD of the tar operation, used for the -C <dir> ' .
                    'CLI option of GNU tar');

has 'tar_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The iRODS collection or filesystem directory path ' .
                    ' in which the tar file(s) will be created');

has 'tar_stream' =>
  (isa           => 'WTSI::NPG::HTS::TarStream',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_tar_stream',
   clearer       => 'clear_tar_stream',
   init_arg      => undef,
   documentation => 'The currently used stream to an open tar file');

has 'remove_files' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 0,
   documentation => 'Enable GNU tar --remove-files option to remove the ' .
                    'original file once archived');

sub BUILD {
  my ($self) = @_;

  # Read any manifest of published files left by a previous process
  if (-e $self->manifest_path) {
    $self->_read_manifest;
  }

  return;
}

sub publish_file {
  my ($self, $path) = @_;

  if ($self->file_published($path)) {
    $self->debug("Skipping '$path'; already published");
  }
  else {
    if (not $self->has_tar_stream) {
      my $tar_file = sprintf '%s.%d.tar', $self->tar_path, $self->tar_count;

      $self->info(sprintf q[Opening '%s' with capacity %d from tar CWD '%s'],
                  $self->tar_path, $self->tar_capacity, $self->tar_cwd);

      $self->tar_stream(WTSI::NPG::HTS::TarStream->new
                        (tar_cwd      => $self->tar_cwd,
                         tar_file     => $tar_file,
                         remove_files => $self->remove_files));
      $self->tar_stream->open_stream;
    }

    $self->tar_stream->add_file($path);

    if ($self->tar_stream->file_count >= $self->tar_capacity) {
      $self->info(sprintf q['%s' reached capacity of '%d'],
                  $self->tar_stream->tar_file, $self->tar_capacity);
      $self->close_stream;
    }
  }

  return $self->manifest_index->{$path};
}

sub file_published {
  my ($self, $path) = @_;

  return exists $self->manifest_index->{$path};
}

sub tar_in_progress {
  my ($self) = @_;

  return ($self->has_tar_stream and $self->tar_stream->file_count > 0);
}

sub close_stream {
  my ($self) = @_;

  if ($self->tar_in_progress) {
    $self->tar_stream->close_stream;
    $self->tar_count($self->tar_count + 1);
    $self->_write_manifest;
    $self->clear_tar_stream;

    $self->debug('Closed tar file #', $self->tar_count);
  }

  return;
}

sub _read_manifest {
  my ($self) = @_;

  my $mpath = $self->manifest_path;
  my $index = $self->manifest_index;

  if (-e $mpath) {
    open my $fh, '<', $mpath or
      $self->logcroak("Failed to open manifest '$mpath' ",
                      "for reading: $ERRNO");

    while (my $line = <$fh>) {
      chomp $line;
      my ($tpath, $ipath) = split /\t/msx, $line;
      $self->debug("Added '$ipath' to manifest index");
      $index->{$ipath} = $tpath;
    }

    close $fh or $self->logcroak("Failed to close '$mpath': $ERRNO");
  }

  return;
}

sub _write_manifest {
  my ($self) = @_;

  if (not $self->has_tar_stream) {
    $self->logconfess('Internal error: attempted to write a manifest ',
                      'of tar contents with no tar stream available');
  }

  my $mpath = $self->manifest_path;
  my $index = $self->manifest_index;
  my $tpath = $self->tar_stream->tar_file;
  my $items = $self->tar_stream->tar_content;

  open my $fh, '>>', $mpath
    or $self->logcroak("Failed to open manifest '$mpath' ",
                       "for appending: $ERRNO");

  foreach my $ipath (sort keys %{$items}) {
    $index->{$ipath} = $tpath;
    print $fh "$tpath\t$ipath\n" or
      $self->logcroak("Failed to write to filehandle of '$mpath'");
  }

  close $fh or $self->logcroak("Failed to close '$mpath': $ERRNO");

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::TarPublisher

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
