package WTSI::NPG::HTS::TarStream;

use namespace::autoclean;

use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel rel2abs];
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

our $ISO8601_DATETIME = '%Y-%m-%dT%H%m%S';
our $PUT_STREAM       = 'npg_irods_putstream.sh';

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
   documentation => 'The paths of files added the current tar archive ' .
                    'mapped to an array of MD5 checksums. The array will ' .
                    'contain 1 checksum for each time that a file was added ' .
                    'with that name');

has 'remove_files' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 0,
   documentation => 'Enable GNU tar --remove-files option to remove the ' .
                    'original file once archived');

has 'time_started' =>
  (isa           => 'DateTime',
   is            => 'ro',
   required      => 1,
   predicate     => 'has_time_started',
   builder       => '_build_time_started',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'The time at which the tar stream was opened');

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

  my $now_datetime = $self->time_started->strftime($ISO8601_DATETIME);
  $self->info("Started tar process to '$tar_path' with PID ",
              "$pid at $now_datetime");

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

  Example    : my $item_path = $obj->add_file('/path/to/file');
  Description: Add a file to the current tar stream and return its
               path relative to the tar CWD (i.e. return its path
               within the archive).
  Returntype : Str

=cut

sub add_file {
  my ($self, $file_path) = @_;

  $self->_check_absolute($file_path);

  my $item_path = abs2rel($file_path, $self->tar_cwd);

  my $size     = -s $file_path;
  my $tar_file = $self->tar_file;
  $self->debug("Adding '$item_path' ($size bytes) to '$tar_file'");

  # Assumes that the file is not modified before it gets tarred
  my $checksum = $self->_calculate_checksum($file_path);

  print {$self->tar} "$item_path\n" or
    $self->logcroak("Failed write to filehandle of '$tar_file'");

  $self->tar_content->{$item_path} ||= [];
  push @{$self->tar_content->{$item_path}}, $checksum;
  $self->byte_count($self->byte_count + $size);

  return $item_path;
}

=head2 file_added

  Arg [1]    : Absolute file path, Str.

  Example    : $obj->file_added('/path/to/file');
  Description: Return true if the file has been added to the tar stream.
  Returntype : Bool

=cut

sub file_added {
  my ($self, $file_path) = @_;

  $self->_check_absolute($file_path);
  my $item_path = abs2rel($file_path, $self->tar_cwd);

  return exists $self->tar_content->{$item_path};
}

=head2 file_updated

  Arg [1]    : Absolute file path, Str.

  Example    : $obj->file_added('/path/to/file');
  Description: Return true if the file has been added to the tar stream
               more than once.
  Returntype : Bool

=cut

sub file_updated {
  my ($self, $file_path) = @_;

  $self->_check_absolute($file_path);

  return scalar $self->file_checksum_history($file_path) > 1;
}

=head2 file_checksum

  Arg [1]    : Absolute file path, Str.

  Example    : my $checksum = $obj->file_checksum('/path/to/file');
  Description: Return the checksum calculated the last time the file
               was added to the tar stream.
  Returntype : Bool

=cut

sub file_checksum {
  my ($self, $file_path) = @_;

  $self->_check_absolute($file_path);

  my $tar_file = $self->tar_file;
  $self->file_added($file_path) or
    $self->logcroak("File '$file_path' has not been added to '$tar_file'");

  my $checksum;
  if ($self->file_added($file_path)) {
    my @checksums = $self->file_checksum_history($file_path);
    $checksum = $checksums[-1];
  }

  return $checksum;
}

=head2 file_path

  Arg [1]    : Relative item path, Str.

  Example    : my $path = $obj->file_path('1.txt');
  Description: Return the absolute path of the file corresponding to the
               specififed tar item (wrt to the tar process CWD).
  Returntype : Str

=cut

sub file_path {
  my ($self, $item_path) = @_;

  my $tar_file = $self->tar_file;
  $self->item_added($item_path) or
    $self->logcroak("File '$item_path' has not been added to '$tar_file'");

  return rel2abs($item_path, $self->tar_cwd);
}

=head2 item_added

  Arg [1]    : Relative item path, Str.

  Example    : $obj->item_added('1.txt');
  Description: Return true if specififed tar item has been added.
  Returntype : Bool

=cut

sub item_added {
  my ($self, $item_path) = @_;

  return exists $self->tar_content->{$item_path};
}

=head2 item_path

  Arg [1]    : Absolute file path, Str.

  Example    : my $path = $obj->item_path('/path/to/file');
  Description: Return the relative item path corresponding to the
               specified file (wrt to the tar process CWD).
  Returntype : Bool

=cut

sub item_path {
  my ($self, $file_path) = @_;

  my $tar_file = $self->tar_file;
  $self->file_added($file_path) or
    $self->logcroak("File '$file_path' has not been added to '$tar_file'");

  return abs2rel($file_path, $self->tar_cwd);
}

=head2 file_checksum_history

  Arg [1]    : Absolute file path, Str.

  Example    : my @checksums = $obj->file_checksum_history('/path/to/file');
  Description: Return the checksums calculated for every occasion the
               file was added to the tar stream.
  Returntype : Array[Str]

=cut

sub file_checksum_history {
  my ($self, $file_path) = @_;

  my $tar_file = $self->tar_file;
  $self->file_added($file_path) or
    $self->logcroak("File '$file_path' has not been added to '$tar_file'");

  my $item_path = abs2rel($file_path, $self->tar_cwd);
  return @{$self->tar_content->{$item_path}};
}

=head2 file_paths

  Arg [1]    : None

  Example    : my @paths = $obj->file_paths
  Description: Return the absolute file path of all files added to the tar
               stream (wrt to the tar process CWD).
  Returntype : Bool

=cut

sub file_paths {
  my ($self) = @_;

  my $tar_cwd = $self->tar_cwd;
  my @file_paths = map { rel2abs($_, $tar_cwd) } $self->item_paths;

  return @file_paths;
}

=head2 item_paths

  Arg [1]    : None

  Example    : my @paths = $obj->item_paths
  Description: Return the relative item paths of all items added to the tar
               stream (wrt to the tar process CWD).
  Returntype : Bool

=cut

sub item_paths {
  my ($self) = @_;

  my @item_paths = keys %{$self->tar_content};
  @item_paths = sort @item_paths;

  return @item_paths;
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

sub _calculate_checksum {
  my ($self, $file_path) = @_;

  open my $in, '<', $file_path or
    $self->logcroak("Failed to open '$file_path' for checksum calculation: ",
                    "$ERRNO");
  binmode $in;

  my $checksum = Digest::MD5->new->addfile($in)->hexdigest;

  close $in or
    $self->warn("Failed to close '$file_path': $ERRNO");

  return $checksum;
}

sub _check_absolute {
  my ($self, $file_path) = @_;

  defined $file_path or
    $self->logconfess('A defined file_path argument is required');
  $file_path =~ m{^/}msx or
    $self->logconfess("An absolute path argument is required: '$file_path'");

  return;
}

sub _build_time_started {
  my ($self) = @_;

  return DateTime->now;
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
