package WTSI::NPG::HTS::ONT::MinIONRunPublisher;

use namespace::autoclean;

use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel catdir catfile rel2abs splitpath];
use IO::Select;
use Linux::Inotify2;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::ArchiveSession
       ];

our $VERSION = '';

our $SELECT_TIMEOUT = 2;
our $ISO8601_BASIC  = '%Y-%m-%dT%H%M%S';

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'minion_id' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_minion_id',
   documentation => 'The ID of a MinION. Only files matching this ID will ' .
                    'be published. This provides a consistency check');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A directory path under which numbered directories of ' .
                    'fast5 files are located');

has 'inotify' =>
  (isa           => 'Linux::Inotify2',
   is            => 'ro',
   required      => 1,
   builder       => '_build_inotify',
   lazy          => 1,
   documentation => 'The inotify instance');

has 'event_queue' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   documentation => 'A queue of Linux::Inotify2::Event objects to be ' .
                    'processed. Inotify callbacks push events here');

has 'watches' =>
  (isa           => 'HashRef',
   is            => 'rw',
   required      => 1,
   default       => sub { return {} },
   documentation => 'A mapping of absolute paths of watched directories '.
                    'to a corresponding Linux::Inotify2::Watch instance');

=head2 publish_files

  Arg [1]    : None

  Example    : my ($num_files, $num_errors) = $pub->publish
  Description: Start a loop watching for files to publish. Return when
               session_timeout seconds have elapsed.
  Returntype : Array[Int]

=cut

sub publish_files {
  my ($self) = @_;

  -e $self->runfolder_path or
    $self->logconfess(q[MinION runfolder path '], $self->runfolder_path,
                      q[' does not exist]);

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  $self->_start_watches;

  # A manifest of loaded read files.
  my $manifest_file;  # The manifest file name
  my $manifest_index; # Used where manifest is loaded by follow-up session

  # The current tar file. There will be one or more tar files per
  # loading session.
  my $tar;            # The file handle for writing
  my $tar_file;       # The current file name
  my %tar_content;    # The read files in the current tar archive, by path
  my $tar_begin = time;

  # The current loading session.
  my $session_name   = DateTime->now->strftime('%Y-%m-%dT%H%m%S');
  my $session_active = $tar_begin;
  my $continue       = 1; # While true, continue loading
  my $tar_count      = 0; # The number of tar files loaded this session
  my $num_errors     = 0;

  # Ensure a clean exit on SIGTERM, so that any partly-created tar file
  # is closed
  local $SIG{TERM} = sub { $continue = 0 };

  try {
    while ($continue) {
      $self->debug('Continue ...');
      if ($select->can_read($SELECT_TIMEOUT)) {
        my $n = $self->inotify->poll;
        $self->debug("$n events");
      }

      if (@{$self->event_queue}) {
        $self->debug(scalar @{$self->event_queue}, ' events in queue');

      EVENT: while (my $event = shift @{$self->event_queue}) {
          $session_active = time; # No longer idle

          my $abs_path = $event->fullname;
          $self->debug("Event on $abs_path");

          if (not $self->_fast5_match($abs_path)) {
            next EVENT;
          }

          my ($hostname, $run_date, $observed_minion_id, $flowcell_id) =
            $self->_parse_file_name($abs_path);

          if (not $self->_minion_match($abs_path, $observed_minion_id)) {
            next EVENT;
          }

          if (not $manifest_file) {
            $manifest_file  = $self->_manifest_path($flowcell_id, $run_date);
            $manifest_index = $self->_load_manifest_index($manifest_file);
          }

          if (not $tar) {
            $tar_file = $self->_tar_path($observed_minion_id, $flowcell_id,
                                         $run_date, $session_name, $tar_count);
            $tar_begin   = time; # tar timer
            $tar         = $self->_open_tar($tar_file);
            %tar_content = ();
          }

          if (exists $manifest_index->{$abs_path}) {
            $self->debug("Skipping '$abs_path'; already loaded");
            next EVENT;
          }

          my $rel_path = abs2rel($abs_path, $self->runfolder_path);
          $self->debug("Adding '$rel_path' to '$tar_file'");
          print $tar "$rel_path\n" or
            $self->logcroak("Failed write to filehandle of '$tar_file'");
          $tar_content{$abs_path} = $tar_file;

          my $file_count    = scalar keys %tar_content;
          my $arch_capacity = $self->arch_capacity;
          if ($file_count >= $arch_capacity) {
            $self->info("'$tar_file' reached capacity of $arch_capacity");

            $tar = $self->_close_fh($tar, $tar_file);
            $tar_count++;
            $manifest_index = $self->_update_manifest($manifest_file,
                                                      $tar_file,
                                                      \%tar_content);
          }
        }
      } # Can read
      else {
        $self->debug("Select timeout ($SELECT_TIMEOUT sec) ...");
        my $now          = time;
        my $elapsed      = $now - $tar_begin; # tar timer
        my $file_count   = scalar keys %tar_content;
        my $arch_timeout = $self->arch_timeout;

        if (defined $tar and $file_count > 0 and $elapsed > $arch_timeout) {
          $self->debug("Archive timeout $arch_timeout reached waiting ",
                       "for more files. Archiving $file_count files in ",
                       "'$tar_file'");

          $tar = $self->_close_fh($tar, $tar_file);
          $tar_count++;
          $manifest_index = $self->_update_manifest($manifest_file,
                                                    $tar_file,
                                                    \%tar_content);
        }

        my $session_idle    = $now - $session_active;
        my $session_timeout = $self->session_timeout;
        if ($session_idle >= $session_timeout) {
          $self->info("Session timeout reached after $session_idle seconds");
          $continue = 0;
        }

        if (defined $tar_file) {
          $self->debug(sprintf 'tar: %s, tar time elapsed: %d / %d, ' .
                               'session idle: %d / %d, files: %d',
                       $tar_file, $elapsed, $self->arch_timeout,
                       $session_idle, $self->session_timeout, $file_count);
        }
        else {
          $self->debug(sprintf 'Session idle: %d / %d', $session_idle,
                       $self->session_timeout);
        }
      } # Read timeout
    } # Continue

    $self->_close_fh($tar, $tar_file);
  } catch {
    $self->error($_);
    $num_errors++;
  };

  $self->_stop_watches;
  $select->remove($self->inotify->fileno);

  return ($tar_count, $num_errors);
}

sub _build_inotify {
  my ($self) = @_;

  my $inotify = Linux::Inotify2->new or
    $self->logcroak("Failed to create a new Linux::Inotify2 object: $ERRNO");

  return $inotify;
}

sub _start_watches {
  my ($self) = @_;

  my $events = IN_CLOSE_WRITE | IN_MOVED_TO | IN_CREATE |
    IN_MOVED_FROM | IN_DELETE;

  $self->_recurse_setup_callbacks($self->runfolder_path, $events);

  return $self;
}

sub _stop_watches {
  my ($self) = @_;

  my $watches = $self->watches;
  foreach my $path (keys %{$watches}) {
    $watches->{$path}->cancel;
  }

  $self->watches({});

  return $self;
}

# Recursive through any pre-existing directories to start watching
# them
sub _recurse_setup_callbacks {
  my ($self, $directory, $events) = @_;

  my $cb = $self->_make_callback($events);
  if ($self->inotify->watch($directory, $events, $cb)) {
    $self->debug("Started watch on '$directory'");
  }
  else {
    $self->logconfess("Failed to start watch on '$directory'");
  }

  opendir my $dh, $directory or
    $self->logconfess("Failed to opendir '$directory': $ERRNO");
  my @dirent = grep { ! m{[.]{1,2}$}msx } readdir $dh;
  closedir $dh or $self->warn("Failed to close '$directory': $ERRNO");

  foreach my $ent (@dirent) {
    my $path = rel2abs("$directory/$ent");
    if (-d $path) {
      $self->_recurse_setup_callbacks($path, $events);
    }
  }

  return;
}

sub _make_callback {
  my ($self, $events) = @_;

  my $inotify     = $self->inotify;
  my $event_queue = $self->event_queue;

  return sub {
    my $event = shift;

    if ($event->IN_Q_OVERFLOW) {
      $self->warn('Some events were lost!');
    }

    # Path was added to the watched hierarchy
    if ($event->IN_CREATE or $event->IN_MOVED_TO) {
      my $path = $event->fullname;

      if ($event->IN_ISDIR) {
        # Path added is a directory; add a watch unless already
        # watched
        if (exists $self->watches->{$path}) {
          $self->debug("Already watching '$path'");
        }
        else {
          my $watch = $inotify->watch($path, $events,
                                      $self->_make_callback($events));
          if ($watch) {
            $self->debug("Started watching '$path'");
            $self->watches->{$path} = $watch;
          }
          else {
            $self->logconfess("Failed to start watching '$path': $ERRNO");
          }
        }
      }
      else {
        # Path added was a file; add the event to the queue, to be
        # handled in the main loop
        $self->debug("Event IN_CREATE/IN_MOVED_TO on '$path'");
        push @{$event_queue}, $event;
      }
    }

    # Path was removed from the watched hierarchy
    if ($event->IN_DELETE or $event->IN_MOVED_FROM) {
      my $path = $event->fullname;

      if ($event->IN_ISDIR) {
        # Path is a directory being watched; remove the watch
        if (exists $self->watches->{$path}) {
          my $watch = delete $self->watches->{$path};
          $watch->cancel;
          $self->debug("Stopped watching '$path'");
        }
      }
      else {
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on '$path'");
      }
    }
  };
}

sub _parse_file_name {
  my ($self, $path) = @_;

  my ($volume, $dirs, $file) = splitpath($path);
  my ($hostname, $run_date, $flowcell_id, $minion_id) = split /_/msx, $file;

  if (not ($hostname and $run_date and $flowcell_id and $minion_id)) {
    $self->logcroak("Failed to parse file name '$file'");
  }

  return ($hostname, $run_date, $minion_id, $flowcell_id);
}

sub _fast5_match {
  my ($self, $path) = @_;

  my $match = $path =~ m{[.]fast5$}msxi;
  if (not $match) {
    $self->warn("Ignoring file (not fast5): '$path'");
  }

  return $match;
}

sub _minion_match {
  my ($self, $path, $observed_minion_id) = @_;

  my $match = 0;

  if ($observed_minion_id) {
    if ($self->has_minion_id) {
      $match = $observed_minion_id eq $self->minion_id;
    }
    else {
      $self->minion_id($observed_minion_id);
      $match = 1;
    }
  }

  if (not $match) {
    $self->warn(qq[Ignoring file (MinION '$observed_minion_id'), ],
                q[expected MinION '], $self->minion_id, qq['): '$path']);
  }

  return $match;
}

sub _manifest_path {
  my ($self, $flowcell_id, $run_date) = @_;

  return catfile($self->runfolder_path, sprintf '%s_%s_%s.txt',
                 $self->minion_id, $flowcell_id, $run_date);
}

sub _load_manifest_index {
  my ($self, $path) = @_;

  my %index;

  if (-e $path) {
    open my $fh, '<', $path or
      $self->logcroak("Failed to open manifest '$path': $ERRNO");

    while (my $line = <$fh>) {
      chomp $line;
      my ($tar_path, $fast5_path) = split /\t/msx, $line;
      $self->debug("Added '$fast5_path' to manifest index");
      $index{$fast5_path} = $tar_path;
    }

    close $fh or $self->logcroak("Failed to close '$path': $ERRNO");
  }

  return \%index;
}

sub _update_manifest {
  my ($self, $path, $tar_path, $items) = @_;

  open my $fh, '>>', $path
    or $self->logcroak("Failed to open '$path' for appending: $ERRNO");

  foreach my $fast5_path (sort keys %{$items}) {
    print $fh "$tar_path\t$fast5_path\n" or
      $self->logcroak("Failed to write to filehandle of '$path'");
  }

  close $fh or $self->logcroak("Failed to close '$path': $ERRNO");

  return $self->_load_manifest_index($path);
}

## no critic (Subroutines::ProhibitManyArgs)
sub _tar_path {
  my ($self, $minion_id, $flowcell_id, $run_date, $session, $tar_count) = @_;

  my $coll = catdir($self->dest_collection, $minion_id, $flowcell_id);

  return catfile($coll, sprintf '%s_%s_%s.%s.%d.tar', $self->minion_id,
                 $flowcell_id, $run_date, $session, $tar_count);
}
## use critic

sub _open_tar {
  my ($self, $path) = @_;

  my ($obj_name, $collections, $suffix) = fileparse($path, qr{[.][^.]*}msx);
  $suffix =~ s/^[.]//msx; # Strip leading dot from suffix

  if (not $suffix) {
    $self->logconfess("Invalid tar file path '$path'");
  }

  my $runfolder_path = $self->runfolder_path;
  my $tar_cmd = qq[tar -C $runfolder_path -c -T - | ] .
                qq[npg_irods_putstream.sh -t $suffix $path];
  $self->info("Opening pipe to '$tar_cmd' in $runfolder_path");

  open my $fh, q[|-], $tar_cmd
    or $self->logcroak("Failed to open pipe to '$tar_cmd': $ERRNO");

  return $fh;
}

sub _close_fh {
  my ($self, $fh, $filename) = @_;

  if (defined $fh) {
    $self->info("Closing '$filename'");

    close $fh or $self->logcroak("Failed to close '$filename': $ERRNO");
    $self->debug("'$filename' was closed");
  }

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::MinIONRunPublisher

=head1 DESCRIPTION

Publishes ONT fast5 files by streaming them through GNU tar and the
tears iRODS client, directly into a series of tar archives in an iRODS
collection.

The fast5 files must be located under a single top level directory
(the 'runfolder'). New directories and files under the runfolder are
detected by inotify.

An instance will only publish fast5 files produced by a single,
specified MinION. If any tar archive takes longer to reach its
capacity than the arch_timeout in seconds, that archive is
automatically closed. Any further fast5 file(s) will be added to a new
archive.

A publishing session is started by calling the 'publish_files' method
which will return when the process is complete. If processing a run
takes longer than session_timeout seconds, any currently open archive
will be closed and published and the publish_files method will
return. Any inotify watches will be released and no further files will
be processed until publish_files is called again.

The publisher will create new collections in iRODS into which the tar
files will be written. These collections will be named

  <dest collection>/<MinION ID>/<Flowcell ID>/

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
