package WTSI::NPG::HTS::TarStream;

use namespace::autoclean;

use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel];
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

our $PUT_STREAM = 'npg_irods_putstream.sh';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

has 'byte_count' =>
  (isa           => 'Int',
   is            => 'rw',
   required      => 1,
   default       => 0,
   init_arg      => undef,
   documentation => 'The total number of bytes published');

has 'pid' =>
  (isa           => 'Int',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_pid',
   clearer       => 'clear_pid',
   init_arg      => undef,
   documentation => 'The child process ID');

has 'tar' =>
  (isa           => 'FileHandle',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_tar',
   clearer       => 'clear_tar',
   init_arg      => undef,
   documentation => 'The tar file handle for writing');

has 'tar_cwd' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The local working directory of the tar operation');

has 'tar_file' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The tar file name');

has 'tar_content' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'The file names added the current tar archive, by path');

has 'remove_files' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 0,
   documentation => 'Enable GNU tar --remove-files option to remove the ' .
                    'original file once archived');

sub BUILD {
  my ($self) = @_;

  $self->_check_absolute($self->tar_cwd);
  $self->_check_absolute($self->tar_file);

  return;
}

=head2 open_stream

  Arg [1]    : None

  Example    : $obj->open_stream
  Description: Open a new stream to a tar file in iRODS. Return the new
               filehandle.
  Returntype : FileHandle

=cut

sub open_stream {
  my ($self) = @_;

  my $tar_path = $self->tar_file;

  my ($obj_name, $collections, $suffix) =
    fileparse($tar_path, qr{[.][^.]*}msx);
  $suffix =~ s/^[.]//msx; # Strip leading dot from suffix

  if (not $suffix) {
    $self->logconfess("Invalid data object path '$tar_path'");
  }

  my $tar_cwd     = $self->tar_cwd;
  my $tar_options = $self->remove_files ? '--remove-files ' : q[];
  $tar_options .= "-C '$tar_cwd' -c -T ";

  my $tar_cmd = "tar $tar_options - | " .
                "$PUT_STREAM -t $suffix '$tar_path' >/dev/null";
  $self->info("Opening pipe to '$tar_cmd' in '$tar_cwd'");

  my $pid = open my $fh, q[|-], $tar_cmd
    or $self->logcroak("Failed to open pipe to '$tar_cmd': $ERRNO");

  $self->info("Started tar process to '$tar_path' with PID $pid");
  $self->tar($fh);
  $self->pid($pid);

  return $self->tar;
}

=head2 close_stream

  Arg [1]    : None

  Example    : $obj->close_stream
  Description: Close any current stream safely.
  Returntype : Undef

=cut
sub close_stream {
  my ($self) = @_;

  my $tar_path = $self->tar_file;
  if ($self->has_tar) {
    my $pid = $self->pid;

    close $self->tar or
      $self->logcroak("Failed close tar process to '$tar_path' with PID ",
                      "$pid: $ERRNO");

    $self->debug("Closed tar process to '$tar_path' with PID $pid");
  }
  $self->clear_tar;
  $self->clear_pid;

  return;
}

=head2 add_file

  Arg [1]    : Absolute file path, Str.

  Example    : my $path = $obj->add_file('/path/to/file');
  Description: Add a file to the current tar stream and return its
               path relative to the tar CWD (i.e. return its path
               within the archive).
  Returntype : Str

=cut

sub add_file {
  my ($self, $path) = @_;

  $self->_check_absolute($path);

  my $rel_path = abs2rel($path, $self->tar_cwd);

  my $size     = -s $path;
  my $filename = $self->tar_file;
  $self->debug("Adding '$rel_path' ($size bytes) to '$filename'");

  print {$self->tar} "$rel_path\n" or
    $self->logcroak("Failed write to filehandle of '$filename'");

  $self->tar_content->{$rel_path} = 1;
  $self->byte_count($self->byte_count + $size);

  return $rel_path;
}

sub file_added {
  my ($self, $path) = @_;

  $self->_check_absolute($path);
  my $rel_path = abs2rel($path, $self->tar_cwd);

  return exists $self->tar_content->{$rel_path};
}

=head2 file_count

  Arg [1]    : None

  Example    : my $n = $obj->file_count;
  Description: Return the number of files added to the archive.
  Returntype : Int

=cut

sub file_count {
  my ($self) = @_;

  return scalar keys %{$self->tar_content};
}

sub _check_absolute {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');
  $path =~ m{^/}msx or
    $self->logconfess("An absolute path argument is required: '$path'");

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::TarStream

=head1 DESCRIPTION

Creates a tar stream into iRODS using GNU tar and tears for data and
baton for iRODS metadata.

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
