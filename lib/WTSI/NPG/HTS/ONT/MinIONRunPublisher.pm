package WTSI::NPG::HTS::ONT::MinIONRunPublisher;

use namespace::autoclean;

use Carp;
use IO::Compress::Bzip2 qw[bzip2 $Bzip2Error];
use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Find;
use File::Path qw[make_path];
use File::Spec::Functions qw[abs2rel catdir catfile rel2abs splitdir splitpath];
use File::Temp;
use IO::Select;
use Linux::Inotify2;
use Moose;
use MooseX::StrictConstructor;
use PDL::IO::HDF5;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;
use WTSI::NPG::HTS::TarPublisher;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::Accountable
         WTSI::NPG::HTS::ArchiveSession
         WTSI::NPG::iRODS::Annotator
         WTSI::NPG::iRODS::Utilities
       ];

our $VERSION = '';

our $SELECT_TIMEOUT        = 2;
our $ISO8601_BASIC         = '%Y-%m-%dT%H%m%S';
our $H5REPACK              = 'h5repack';

our $FILE_COMPLETE_RETRIES = 3;
our $FILE_COMPLETE_BACKOFF = 2;

our $FAST5_GLOBAL_GROUP     = 'UniqueGlobalKey';
our $FAST5_TRACKING_GROUP   = 'tracking_id';
our $FAST5_DEVICE_ID_ATTR   = 'device_id';
our $FAST5_RUN_ID_ATTR      = 'run_id';
our $FAST5_SAMPLE_ID_ATTR   = 'sample_id';
our $FAST5_SOFTWARE_VERSION = 'version';

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A directory path under which sequencing result files ' .
                    'are located');
has 'run_id' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_run_id',
   init_arg      => undef,
   documentation => 'The MinKNOW run identifier');

has 'sample_id' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_sample_id',
   init_arg      => undef,
   documentation => 'The MinKNOW sample identifier');

has 'session_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => sub { return DateTime->now->strftime($ISO8601_BASIC) },
   documentation => 'A session name to be used as a component of output ' .
                    'file names. Defaults to an ISO8601 date.');

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

has 'f5_publisher' =>
  (isa           => 'WTSI::NPG::HTS::TarPublisher',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_f5_publisher',
   init_arg      => undef,
   lazy          => 1,
   builder       => '_build_f5_publisher',
   documentation => 'The tar publisher for fast5 files');

has 'fq_publisher' =>
  (isa           => 'WTSI::NPG::HTS::TarPublisher',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_fq_publisher',
   init_arg      => undef,
   lazy          => 1,
   builder       => '_build_fq_publisher',
   documentation => 'The tar publisher for fastq files');

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_irods',
   documentation => 'An iRODS handle to run searches and perform updates');

has 'tmpfs' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => '/tmp',
   documentation => 'Temporary filesystem for file manipulation');

has 'tmpdir' =>
  (isa           => 'File::Temp::Dir',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_tmpdir',
   clearer       => 'clear_tmpdir',
   lazy          => 1,
   builder       => '_build_tmpdir',
   documentation => 'Fast temporary directory for file manipulation');

has 'f5_uncompress' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 0,
   documentation => 'Export the Fast5 files without internal compression');

has 'f5_bzip2' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 1,
   documentation => 'Externally compress the Fast5 files with bzip2');

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

  my $dest = $self->dest_collection;
  if ($self->irods->is_collection($dest)) {
    $self->debug("Using existing destination collection '$dest'");
  }
  else {
    $self->debug("Creating new destination collection '$dest'");
    $self->irods->add_collection($dest);
  }

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  $self->_start_watches;

  # The current loading session.
  my $session_active = time; # Session start
  my $continue       = 1;    # While true, continue loading
  my $num_errors     = 0;    # The number of errors this session
  my $session_closed = 0;
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
          $self->debug("Event on '$abs_path'");
          $self->_do_publish($abs_path);
        }
      } # Can read
      else {
        $self->debug("Select timeout ($SELECT_TIMEOUT sec) ...");
        my $now          = time;
        my $session_idle = $now - $session_active;

        if ($session_idle > $self->arch_timeout) {
          $self->debug("Tar timeout reached after $session_idle seconds");

          if ($self->has_f5_publisher) {
            $self->f5_publisher->close_stream;
          }
          if ($self->has_fq_publisher) {
            $self->fq_publisher->close_stream;
          }
        }

        if ($session_idle >= $self->session_timeout) {
          $self->info("Session timeout reached after $session_idle seconds");

          $continue = 0;
        }
      } # Read timeout
    } # Continue

    # Here we run a final check of all files present in the runfolder
    # to catch any we missed. See POD for explanation.
    $self->_catchup;

    if ($self->has_f5_publisher) {
      $self->f5_publisher->close_stream;
    }
    if ($self->has_fq_publisher) {
      $self->fq_publisher->close_stream;
    }
  } catch {
    $self->error($_);
    $num_errors++;
  };

  $self->_stop_watches;
  $select->remove($self->inotify->fileno);

  my $tar_count = 0;
  if ($self->has_f5_publisher) {
    $tar_count += $self->f5_publisher->tar_count;
  }
  if ($self->has_fq_publisher) {
    $tar_count += $self->fq_publisher->tar_count;
  }

  $self->clear_tmpdir;

  return ($tar_count, $num_errors);
}

after 'publish_files' => sub {
  my ($self) = @_;

  my $run_coll = catdir($self->dest_collection, $self->run_id);
  my $coll = WTSI::NPG::iRODS::Collection->new($self->irods, $run_coll);
  my $path = $coll->str;

  my @metadata = $self->make_creation_metadata($self->affiliation_uri,
                                               DateTime->now,
                                               $self->accountee_uri);
  push @metadata, $self->make_avu($ID_RUN, $self->run_id);

  if ($self->has_sample_id) {
    push @metadata, $self->make_avu($SAMPLE_NAME, $self->sample_id);
  }
  else {
    $self->warn(q[Failed to determine a sample_id in runfolder '],
                $self->runfolder_path, "' for collection '$path'");
  }

  my $num_errors = 0;
  foreach my $avu (@metadata) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    my $units = $avu->{units};

    try {
      $coll->supersede_avus($attr, $value, $units);
    } catch {
      my @stack = split /\n/msx;
      $num_errors++;
      $self->error("Failed to supersede AVU on '$path' with attribute ",
                   "'$attr' and value '$value': ", pop @stack);
    };
  }

  if ($num_errors > 0) {
    $self->logcroak("$num_errors errors while setting metadata on '$path'");
  }
};

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

  my @dirs = grep { -d } map { rel2abs($_, $directory) } @dirent;
  foreach my $dir (@dirs) {
    $self->_recurse_setup_callbacks($dir, $events);
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

    my $path = $event->fullname;

    if ($event->IN_ISDIR) {
      if ($event->IN_CREATE or $event->IN_MOVED_TO) {
        # Directory was added to the watched hierarchy; if not already
        # watched, add a watch
        if (exists $self->watches->{$path}) {
          $self->debug("Already watching dir '$path'");
        }
        else {
          my $watch = $inotify->watch($path, $events,
                                      $self->_make_callback($events));
          if ($watch) {
            $self->debug("Started watching dir '$path'");
            $self->watches->{$path} = $watch;
          }
          else {
            $self->logconfess("Failed to start watching dir '$path': $ERRNO");
          }
        }
      }
      elsif ($event->IN_DELETE or $event->IN_MOVED_FROM) {
        # Directory was removed from the watched hierarchy; if watched,
        # remove the watch
        if (exists $self->watches->{$path}) {
          my $watch = delete $self->watches->{$path};
          $watch->cancel;
          $self->debug("Stopped watching dir '$path'");
        }
      }
      else {
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on dir '$path'");
      }
    }
    else {
      # Path added was a file; add the event to the queue, to be
      # handled in the main loop
      if ($event->IN_MOVED_TO or $event->IN_CLOSE_WRITE) {
        $self->debug("Event IN_MOVED_TO/IN_CLOSE_WRITE on file '$path'");
        push @{$event_queue}, $event;
      }
      else {
        $self->debug("Ignoring uninteresting event on file '$path'");
      }
    }
  };
}

sub _identify_run_fast5 {
  my ($self, $path) = @_;

  my $run_id;
  my $sample_id;
  my $device_id = 'unknown_device_id';
  my $version   = 'unknown_minknow_version';

  my $f5;
  my $retries = 0;
  my $backoff = 1;

  # If we are responding to IN_CREATE/IN_MOVED_TO, the file may not be
  # fully written
  while (not defined $f5 and $retries < $FILE_COMPLETE_RETRIES) {
    sleep $backoff;
    $f5 = PDL::IO::HDF5->new($path);
    $backoff *= $FILE_COMPLETE_BACKOFF;
    $retries++;
  }

  try {
    my $tracking_group =
      $f5->group($FAST5_GLOBAL_GROUP)->group($FAST5_TRACKING_GROUP);

    if ($tracking_group) {
      ($device_id) = $tracking_group->attrGet($FAST5_DEVICE_ID_ATTR);
      ($run_id)    = $tracking_group->attrGet($FAST5_RUN_ID_ATTR);
      ($sample_id) = $tracking_group->attrGet($FAST5_SAMPLE_ID_ATTR);
      ($version)   = $tracking_group->attrGet($FAST5_SOFTWARE_VERSION);

      $self->debug("$device_id $run_id $version $sample_id");
    }
    else {
      croak "Failed to read the tracking group from '$path'";
    }

    if (not $run_id) {
      croak "Failed to read the run ID from '$path'";
    }
  } catch {
    $self->error("Failed to read from fast5 file '$path': $_");
  };

  return ($run_id, $sample_id);
}

sub _identify_run_fastq {
  my ($self, $path) = @_;

  my $run_id;
  my $sample_id; # TODO -- Always undef at the moment; configure
                 # Albacore to put this in the Fastq header

  try {
    open my $fh, '<', $path or croak "Failed to open '$path': $ERRNO";
    my $header = <$fh>;

    ($run_id) = $header =~ m{^@\S+\s+runid=(\S+)}msx;
    if (not $run_id) {
      croak "Failed to read the run ID from '$path'";
    }
  } catch {
    $self->error("Failed to read from Fastq file '$path': $_");
  };

  return ($run_id, $sample_id);
}

sub _do_publish {
  my ($self, $path) = @_;

  my ($format) = $path =~ qr{[.]([^.]*)}msx;
  if ($format ne 'fast5' and $format ne 'fastq') {
    $self->debug("Ignoring '$path'");
  }
  else {
  CASE: {
      my ($vol, $relative_path, $filename) =
        splitpath(abs2rel($path, $self->runfolder_path));

      my $tmp_dir  = catdir($self->tmpdir, $relative_path);
      my $tmp_path = catfile($tmp_dir, $filename);

      make_path($tmp_dir);
      copy($path, $tmp_path) or
        $self->logcroak("Failed to copy '$path' to '$tmp_path': ", $ERRNO);

      if (-e $tmp_path) {
        $self->debug("'$tmp_path' exists");
      }
      else {
        $self->logcroak("'$tmp_path' has disappeared!");
      }

      if ($format =~ /fast5/msx) {
        if (not ($self->has_run_id and $self->has_sample_id)) {
          my ($rid, $sid) = $self->_identify_run_fast5($path);
          $self->run_id($rid);
          $self->sample_id($sid);
        }

        if ($self->f5_uncompress) {
          $tmp_path = $self->_h5repack_filter($tmp_path, "$tmp_path.repacked");
        }

        if ($self->f5_bzip2) {
          $tmp_path = $self->_bzip2_filter($tmp_path, "$tmp_path.bz2");
        }

        # Don't unlink the file yet because the tar will process it
        # asynchronously. Use the 'remove_file' attribute on the tar
        # stream to recover space.
        $self->f5_publisher->publish_file($tmp_path);

        last CASE;
      }

      if ($format =~ /fastq/msx) {
        if (not ($self->has_run_id)) {
          my ($rid, $sid) = $self->_identify_run_fastq($path);
          $self->run_id($rid);
          # $self->sample_id($sid); # Currently unavailable
        }

        $tmp_path = $self->_bzip2_filter($tmp_path, "$tmp_path.bz2");

        # Don't unlink the file yet
        $self->fq_publisher->publish_file($tmp_path);

        last CASE;
      }
    }
  }

  return;
}

sub _h5repack_filter {
  my ($self, $in_path, $out_path) = @_;

  WTSI::DNAP::Utilities::Runnable->new
      (executable => $H5REPACK,
       arguments  => ['-f', 'SHUF', '-f', 'GZIP=0',
                      $in_path, $out_path])->run;
  $self->debug("Repacked '$in_path' to '$out_path'");

  move($out_path, $in_path) or
    $self->logcroak("Failed to move '$out_path' to '$in_path': ", $ERRNO);

  return $in_path;
}

sub _bzip2_filter {
  my ($self, $in_path, $out_path) = @_;

  bzip2 $in_path => $out_path or
    $self->logcroak("Failed to compress '$in_path': $Bzip2Error");
  $self->debug("Compressed '$in_path' to '$out_path'");

  unlink $in_path;

  return $out_path;
}

sub _catchup {
  my ($self) = @_;

  foreach my $format (qw[fast5 fastq]) {
    $self->info("Catching up any missed '$format' files");
    my @files;
    find(sub {
           my ($f) = qr{[.]([^.]+$)}msx;
           if ($f and $f eq $format) {
             push @files, $File::Find::name
           }
         },
         $self->runfolder_path);

    foreach my $file (@files) {
      $self->debug("Catching up '$file'");
      $self->_do_publish($file);
    }
  }

  return;
}

sub _build_irods {
  my ($self) = @_;

  return WTSI::NPG::iRODS->new;
}

sub _build_tmpdir {
  my ($self) = @_;

  return File::Temp->newdir('MinIONRunPublisher.' . $PID . '.XXXXXXXXX',
                            DIR => $self->tmpfs, CLEANUP => 1);
}

sub _build_f5_publisher {
  my ($self) = @_;

  if (not $self->has_run_id) {
    $self->logconfess('Invalid internal state: run_id not set; ',
                      'cannot build a tar publisher');
  }
  return $self->_make_tar_publisher('fast5');
}

sub _build_fq_publisher {
  my ($self) = @_;

  if (not $self->has_run_id) {
    $self->logconfess('Invalid internal state: run_id not set; ',
                      'cannot build a tar publisher');
  }
  return $self->_make_tar_publisher('fastq');
}

sub _make_tar_publisher {
  my ($self, $format) = @_;

  # The manifest file has the same name across all sessions of
  # publishing a run. This means that repeated sessions will
  # incrementally publish the results of the run, or do nothing if it
  # is already completely published.
  my $manifest_path = catfile($self->runfolder_path,
                              sprintf '%s_%s_manifest.txt',
                              $self->run_id, $format);

  my $run_coll = catdir($self->dest_collection, $self->run_id);
  if ($self->irods->is_collection($run_coll)) {
    $self->debug("Using existing run collection '$run_coll'");
  }
  else {
    $self->debug("Creating new run collection '$run_coll'");
    $self->irods->add_collection($run_coll);
  }

  my $tar_path = catfile($run_coll,
                         sprintf '%s_%s_%s',
                         $self->run_id, $self->session_name, $format);
  # Work in the tmpdir so that the tarred files have the same relative
  # path as if working in the runfolder.
  my $tar_cwd  = $self->tmpdir->dirname;

  return WTSI::NPG::HTS::TarPublisher->new
    (manifest_path => $manifest_path,
     remove_files  => 1,
     tar_bytes     => $self->arch_bytes,
     tar_capacity  => $self->arch_capacity,
     tar_cwd       => $tar_cwd,
     tar_path      => $tar_path);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::MinIONRunPublisher

=head1 DESCRIPTION

Publishes ONT fast5 and fastq files by streaming them through GNU tar
and the tears iRODS client, directly into a series of tar archives in
an iRODS collection.

The fast5 and fastq files must be located under a single top level
directory (the 'runfolder'). Directories and files under the
runfolder are monitored by recursively adding new inotify watches.

The publisher will attempt to capture the sample_id (or "experiment
name" for GridION) and run_id for a run. It does this by examining the
tracking_id group within each Fast5 file. Once the sample_id and
run_id are established, the publisher assumes that they apply to the
entire run.

If Fast5 files are unavailable, the publisher attempts to read the
run_id from the header of each Fastq file. Again, once the run_id is
established, the publisher assumes that it applies to the entire
run. As the Fastq header does not contain a sample_id, it will not be
captured.

All tar files will be written to 'dest_collection'. Metadata will be
added to dest_collection:

  'id_run'            => MinION run_id
  'sample'            => MinION sample_id (if available)
  'dcterms:creator'   => URI
  'dcterms:created'   => Timestamp
  'dcterms:publisher' => URI

Metadata will also be added to each tar file, but only minimally so:


  'dcterms:created'   => Timestamp
  'md5'               => MD5
  'type'              => File suffix

If any tar archive takes longer to reach its capacity than the
arch_timeout in seconds, that archive is automatically closed. Any
further file(s) will be added to a new archive.

A publishing session is started by calling the 'publish_files' method
which will return when the process is complete. If processing a run
takes longer than session_timeout seconds, any currently open archive
will be closed and published and the publish_files method will
return. Any inotify watches will be released and no further files will
be processed until publish_files is called again.

The publisher will place all tar files will be written in the
specified destination collection. It is the responsibilty of the
calling code to manage which destination collection is used.

N.B. The will inevitably be a delay setting up inotify watches on
newly created directories. It is possible that files will be created
during this period and therefore their events missed.

When the publish_file method is about to exit, it will perform a
search for all files under the runfolder and attempt to publish every
one. If they have already been published, the underlying TarPublisher
will skip them because they will be present in its manifest.

=head1 BUGS

This class uses inotify to detect when data files to be published are
closed after writing. If files are written before inotify watches are
set up, they will not be detected and published.

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
