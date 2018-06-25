package WTSI::NPG::HTS::ONT::GridIONRunPublisher;

use namespace::autoclean;

use Carp;
use IO::Compress::Bzip2 qw[bzip2 $Bzip2Error];
use Data::Dump qw[pp];
use DateTime;
use Digest::MD5;
use English qw[-no_match_vars];
use File::Basename;
use File::Copy;
use File::Find;
use File::Path qw[make_path];
use File::Spec::Functions qw[abs2rel catdir catfile rel2abs
                             splitdir splitpath];
use File::Temp;
use IO::Select;
use Linux::Inotify2;
use Moose;
use MooseX::StrictConstructor;
use PDL::IO::HDF5;
use Sys::Hostname;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;
use WTSI::NPG::HTS::ONT::GridIONRun;
use WTSI::NPG::HTS::ONT::TarDataObject;
use WTSI::NPG::HTS::TarPublisher;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS::Publisher;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::Accountable
         WTSI::NPG::iRODS::Annotator
         WTSI::NPG::iRODS::Utilities
         WTSI::NPG::HTS::ArchiveSession
         WTSI::NPG::HTS::ChecksumCalculator
         WTSI::NPG::HTS::ONT::Annotator
         WTSI::NPG::HTS::ONT::Watcher
       ];

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

our $VERSION = '';

our $SELECT_TIMEOUT        = 2;

our $ISO8601_DATE          = '%Y-%m-%d';
our $ISO8601_DATETIME      = '%Y-%m-%dT%H%m%S';
our $H5REPACK              = 'h5repack';

## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
our $FILE_COMPLETE_RETRIES = 3;
## use critic
our $FILE_COMPLETE_BACKOFF = 2;

our $FAST5_GLOBAL_GROUP     = 'UniqueGlobalKey';
our $FAST5_TRACKING_GROUP   = 'tracking_id';
our $FAST5_DEVICE_ID_ATTR   = 'device_id';
our $FAST5_RUN_ID_ATTR      = 'run_id';
our $FAST5_SAMPLE_ID_ATTR   = 'sample_id';
our $FAST5_SOFTWARE_VERSION = 'version';

has 'catchup' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 0,
   documentation => 'True if the publisher is catching up any unpublished ' .
                    'files at the end of its processing');

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

has 'f5_bzip2' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 1,
   documentation => 'Externally compress the Fast5 files with bzip2');

has 'f5_publisher' =>
  (isa           => 'WTSI::NPG::HTS::TarPublisher',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_f5_publisher',
   init_arg      => undef,
   lazy          => 1,
   builder       => '_build_f5_publisher',
   documentation => 'The tar publisher for fast5 files');

has 'f5_uncompress' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 0,
   documentation => 'Export the Fast5 files without internal compression');

has 'file_queue' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   documentation => 'A queue of files to be processed');

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

has 'monitor' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 1,
   documentation => 'While true, continue monitoring the source_dir. ' .
                    'A caller may set this to false in order to stop ' .
                    'monitoring and wait for any child processes to finish');

has 'single_server' =>
  (is            => 'ro',
   isa           => 'Bool',
   default       => 0,
   documentation => 'If true, connect ony a single iRODS server by avoiding ' .
                    'any direct connections to resource servers. This mode ' .
                    'will be much slower to transfer large files, but does ' .
                    'not require resource servers to be accessible');

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

has 'extra_tar_context' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 1,
   default       => 1,
   documentation => 'Include the experiment name and device ID components of ' .
                    'file paths within the tar file. Default is yes. All ' .
                    'new data should do this. Earlier data did not.');


around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (not ref $args[0]) {
    my %args = @args;

    my $gridion_name = delete $args{gridion_name};
    $gridion_name ||= hostname;

    my $run = WTSI::NPG::HTS::ONT::GridIONRun->new
      (device_id       => delete $args{device_id},
       experiment_name => delete $args{experiment_name},
       gridion_name    => $gridion_name,
       output_dir      => delete $args{output_dir},
       source_dir      => delete $args{source_dir});

    return $class->$orig(gridion_run => $run, %args);
  }
  else {
    return $class->$orig(@args);
  }
};

=head2 publish_files

  Arg [1]    : None

  Example    : my ($num_files, $num_errors) = $pub->publish
  Description: Start a loop watching for files to publish. Return when
               session_timeout seconds have elapsed.
  Returntype : Array[Int]

=cut

sub publish_files {
  my ($self) = @_;

  -e $self->output_dir or
    $self->logconfess(sprintf q[Output directory '%s' does not exist],
                      $self->output_dir);
  -d $self->output_dir or
    $self->logconfess(sprintf q[Output directory '%s' is not a directory],
                      $self->output_dir);

  -e $self->source_dir or
    $self->logconfess(sprintf q[Data directory '%s' does not exist],
                      $self->source_dir);
  -d $self->source_dir or
    $self->logconfess(sprintf q[Data directory '%s' is not a directory],
                      $self->source_dir);

  my $dest = $self->dest_collection;
  if ($self->irods->is_collection($dest)) {
    $self->debug("Using existing destination collection '$dest'");
  }
  else {
    $self->debug("Creating new destination collection '$dest'");
    $self->irods->add_collection($dest);
  }

  $self->info(sprintf
              q[Started GridIONPublisher with ] .
              q[source dir: '%s', output dir: '%s', ] .
              q[tar capacity: %d files or %d bytes, tar timeout %d sec, ] .
              q[tar duration: %d, session timeout %d sec],
              $self->source_dir, $self->output_dir,
              $self->arch_capacity, $self->arch_bytes, $self->arch_timeout,
              $self->arch_duration, $self->session_timeout);

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  $self->_start_watches;

  # The current loading session.
  my $session_active = time; # Session start
  my $continue       = 1;    # While true, continue loading
  my $num_files      = 0;
  my $num_processed  = 0;
  my $num_errors     = 0;    # The number of errors this session
  my $session_closed = 0;

  try {
    while ($continue) {
      $self->debug('Continue ...');
      $self->_close_f5_on_duration; # Close if max duration reached
      $self->_close_fq_on_duration; # Close if max duration reached

      if ($select->can_read($SELECT_TIMEOUT)) {
        my $n = $self->inotify->poll;
        $self->debug("$n events");

        if (@{$self->file_queue}) {
          $self->debug(scalar @{$self->file_queue}, ' files in queue');

          while (my $file = shift @{$self->file_queue}) {
            $session_active = time; # No longer idle
            $self->debug("Event on '$file'");
            $self->_do_publish($file);
          }
        }
      } # Can read
      else {
        $self->debug("Select timeout ($SELECT_TIMEOUT sec) ...");
        $self->_close_f5_on_duration; # Close if max duration reached
        $self->_close_fq_on_duration; # Close if max duration reached

        my $now          = time;
        my $session_idle = $now - $session_active;

        if ($session_idle >= $self->session_timeout) {
          $self->info("Session timeout reached after $session_idle seconds");
          $self->_close_all;

          $continue = 0;
        }
      } # Read timeout
    } # Continue

    # Here we run a final check of all files present in the source_dir
    # to catch any we missed. See POD for explanation.
    $self->_catchup;
    $self->_close_all;
  } catch {
    $self->error($_);
    $num_errors++;
  };

  # Tar manifest, sequence_summary_n.txt and configuration.cfg files
  my ($nf, $np, $ne)= $self->_publish_ancillary_files;
  $self->debug("Ancillary file publishing returned [$nf, $np, $ne]");
  $num_files     += $nf;
  $num_processed += $np;
  $num_errors    += $ne;

  # Metadata
  my ($nfa, $npa, $nea) = $self->_add_metadata;
  $self->debug("Metadata operations returned [$nfa, $npa, $nea]");
  $num_errors += $nea;

  $self->stop_watches;
  $select->remove($self->inotify->fileno);

  if ($self->has_f5_publisher) {
    $num_files     += $self->f5_publisher->tar_count;
    $num_processed += $num_files;
  }
  if ($self->has_fq_publisher) {
    $num_files     += $self->fq_publisher->tar_count;
    $num_processed += $num_files;
  }

  $self->clear_wdir;

  return ($num_files, $num_files, $num_errors);
}

sub _list_published_tar_paths {
  my ($self) = @_;

  my $coll = catdir($self->dest_collection, $self->gridion_name,
                    $self->experiment_name, $self->device_id);
  my ($obj_paths) = $self->irods->list_collection($coll);

  my @tar_paths = sort grep { m{[.]tar$}msx } @{$obj_paths};

  return @tar_paths;
}

sub _add_metadata {
  my ($self) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my @primary_avus;

  try {
    @primary_avus = $self->make_primary_metadata($self->experiment_name,
                                                 $self->device_id);
  } catch {
    $self->error($_);
    $num_errors++;
  };

  my @tar_paths;
  try {
    @tar_paths = $self->_list_published_tar_paths;
    $num_files = scalar @tar_paths;
  } catch {
    $self->error($_);
    $num_errors++;
  };

  try {
    foreach my $path (@tar_paths) {
      my ($filename, $collection) = fileparse($path);
      my $obj = WTSI::NPG::HTS::ONT::TarDataObject->new
        (collection  => $collection,
         data_object => $filename,
         irods       => $self->irods);

      $self->debug("Adding primary metadata to '$path'");
      my ($num_pattr, $num_pproc, $num_perr) =
        $obj->set_primary_metadata(@primary_avus);

      if ($num_perr > 0) {
        croak("Failed to set primary metadata cleanly on '$path'");
      }

      $num_processed++;
    }
  } catch {
    $self->error($_);
    $num_errors++;
  };

  return ($num_files, $num_processed, $num_errors);
}

sub _start_watches {
  my ($self) = @_;

  my $events = IN_CLOSE_WRITE | IN_MOVED_TO | IN_CREATE |
    IN_MOVED_FROM | IN_DELETE;
  my $cb = $self->_make_callback($events);

  my $spath = rel2abs($self->source_dir);
  $self->_start_watches_recur($spath, $events, $cb);

  return;
}

# Recursive through any pre-existing directories to start watching
# them
sub _start_watches_recur {
  my ($self, $directory, $events, $callback) = @_;

  $self->start_watch($directory, $events, $callback);

  opendir my $dh, $directory or
    $self->logconfess("Failed to opendir '$directory': $ERRNO");
  my @dirent = grep { ! m{[.]{1,2}$}msx } readdir $dh;
  closedir $dh or $self->warn("Failed to close '$directory': $ERRNO");

  my @dirs = grep { -d } map { rel2abs($_, $directory) } @dirent;
  foreach my $dir (@dirs) {
    $self->_start_watches_recur($dir, $events, $callback);
  }

  return;
}

sub _make_callback {
  my ($self, $events) = @_;

  my $file_queue = $self->file_queue;

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
        try {
          my $cb = $self->_make_callback($events);
          $self->start_watch($path, $events, $cb);
        } catch {
          $self->error($_);
        };
      }
      elsif ($event->IN_DELETE or $event->IN_MOVED_FROM) {
        # Directory was removed from the watched hierarchy; if watched,
        # remove the watch
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on dir '$path'");
        $self->stop_watch($path);
      }
      else {
        $self->debug("Ignoring uninteresting event on directory '$path'");
      }
    }
    else {
      # Path added was a file; add the event to the queue, to be
      # handled in the main loop
      if ($event->IN_MOVED_TO or $event->IN_CLOSE_WRITE) {
        $self->debug("Event IN_MOVED_TO/IN_CLOSE_WRITE on file '$path'");
        push @{$file_queue}, $path;
      }
      else {
        $self->debug("Ignoring uninteresting event on file '$path'");
      }
    }
  };
}

sub _do_publish {
  my ($self, $path) = @_;

  my ($format) = $path =~ qr{[.]([^.]*$)}msx;
  if ($format ne 'fast5' and $format ne 'fastq') {
    $self->debug("Ignoring '$path' because it is not fast5/fastq");
  }
  elsif ($format eq 'fastq' and not $self->catchup) {
    $self->debug("Ignoring '$path' because it is fastq and not in catchup");
  }
  else {
    # Ensure that experiment_name and device_id appear in the
    # relative path in the temporary workspace and therefore also in
    # the tar file by removing them from the base used to calculate
    # the relative path.
    my @dirs = splitdir($self->source_dir);

    if ($self->extra_tar_context) {
      my $did  = pop @dirs; # device_id
      my $exp  = pop @dirs; # experiment name
      if (not @dirs) {
        $self->logcroak('No source_dir root remains after trimming');
      }

      $self->debug('Including in tar file experiment_name ',
                   "'$exp' and device_id '$did'");
    }

    my $relative_to = catdir(@dirs);
    $self->debug("Calculating temporary paths relative to '$relative_to'");

    my ($vol, $relative_path, $filename) =
      splitpath(abs2rel($path, $relative_to));
    $self->debug("Working on path '$path': relative path is ",
                 "'$relative_path', file name is '$filename'");

    my $tmp_dir  = catdir($self->wdir, $relative_path);
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
      $self->_do_publish_f5($path, $tmp_path);
    }
    elsif ($format =~ /fastq/msx) {
      $self->_do_publish_fq($path, $tmp_path);
    }
  }

  return;
}

sub _do_publish_f5 {
  my ($self, $path, $tmp_path) = @_;

  if (not ($self->has_experiment_name and $self->has_device_id)) {
    my ($device_id, $ename) = $self->_identify_run_f5($path);
    $self->experiment_name($ename);
    $self->device_id($device_id);
  }

  if ($self->f5_publisher->file_published($tmp_path) or
      $self->f5_publisher->file_published("$tmp_path.bz2")) {
    # It's already published, so we did nothing. Unlink the temp copy
    # immediately to save space
    unlink $tmp_path;
  }
  else {
    if ($self->f5_uncompress) {
      $tmp_path = $self->_h5repack_filter($tmp_path);
    }

    my $checksum = $self->calculate_checksum($tmp_path);
    if ($self->f5_bzip2) {
      $tmp_path = $self->_bzip2_filter($tmp_path);
    }

    # Not checking for f5_publisher->file_updated because we do
    # not observe fast5 files being updated.

    # Don't unlink the file yet because the tar will process it
    # asynchronously. Use the 'remove_file' attribute on the tar
    # stream to recover space.
    $self->f5_publisher->publish_file($tmp_path, $checksum);
  }

  return;
}

sub _do_publish_fq {
  my ($self, $path, $tmp_path) = @_;

  if (not ($self->has_experiment_name and $self->has_device_id)) {
    my ($device_id, $ename) = $self->_identify_run_fq($path);
    $self->experiment_name($ename);
    $self->device_id($device_id);
  }

  my $checksum = $self->calculate_checksum($tmp_path);
  if ($self->fq_publisher->file_published("$tmp_path.bz2")) {
    $self->warn("'$path' published previously");
    $tmp_path = $self->_bzip2_filter($tmp_path);

    if ($self->fq_publisher->file_updated($tmp_path, $checksum)) {
      $self->warn("'$path' has been updated while publishing");
      $self->fq_publisher->publish_file($tmp_path, $checksum); # Don't unlink
    }
    else {
      # The file was published before and hadn't been updated, so we
      # did nothing. Unlink the temp copy immediately to save space.
      unlink $tmp_path;
    }
  }
  else {
    $self->debug("'$path' not published previously");
    $tmp_path = $self->_bzip2_filter($tmp_path);
    $self->fq_publisher->publish_file($tmp_path, $checksum); # Don't unlink
  }

  return;
}

sub _h5repack_filter {
  my ($self, $in_path) = @_;

  my $out_path = "$in_path.repacked";
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
  my ($self, $in_path) = @_;

  my $out_path = "$in_path.bz2";
  bzip2 $in_path => $out_path or
    $self->logcroak("Failed to compress '$in_path': $Bzip2Error");
  $self->debug("Compressed '$in_path' to '$out_path'");

  unlink $in_path;

  return $out_path;
}

sub _catchup {
  my ($self) = @_;

  my $dir = $self->source_dir;
  $self->info("Catching up any missed files under '$dir', recursively");

  try {
    $self->catchup(1);

    my @f5_files = @{$self->gridion_run->list_f5_files};
    $self->info('Catching up ', scalar @f5_files, ' fast5 files');
    my @fq_files = @{$self->gridion_run->list_fq_files};
    $self->info('Catching up ', scalar @fq_files, ' fastq files');

    foreach my $file (@f5_files, @fq_files) {
      $self->debug("Catching up '$file'");
      $self->_do_publish($file);
    }
    $self->info('Catchup done');
  } finally {
    $self->catchup(0);
  };

  return;
}

sub _publish_ancillary_files {
  my ($self) = @_;

  # This is a hack. At this time there is no flag to disable md5 cache
  # files. However, we can limit their creation to files above a
  # certain size and make that size unfeasibly large.
  my $publisher = WTSI::NPG::iRODS::Publisher->new
    (checksum_cache_threshold => 1_000_000_000_000,
     irods                    => $self->irods);

  my @files = (@{$self->gridion_run->list_manifest_files},
               @{$self->gridion_run->list_seq_summary_files},
               @{$self->gridion_run->list_seq_cfg_files});

  my ($num_files, $num_processed, $num_errors) = (scalar @files, 0, 0);

  my $coll = catdir($self->dest_collection, $self->gridion_name,
                    $self->experiment_name, $self->device_id);

  foreach my $file (@files) {
    try {
      my $filename = fileparse($file);
      my $dest = catfile($coll, $filename);
      $publisher->publish($file, $dest);
      $num_processed++;
    } catch {
      $self->error($_);
      $num_errors++;
    };
  }

  return ($num_files, $num_processed, $num_errors);
}

sub _identify_run_f5 {
  my ($self, $path) = @_;

  my $sample_id;
  my $device_id;

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
      ($sample_id) = $tracking_group->attrGet($FAST5_SAMPLE_ID_ATTR);

      $self->debug("Fast5 file '$path' device_id: '$device_id', ",
                   "sample_id: '$sample_id'");
    }
    else {
      croak "Failed to read the tracking group from '$path'";
    }
  } catch {
    $self->error("Failed to read from fast5 file '$path': $_");
  };

  return ($device_id, $sample_id);
}

sub _identify_run_fq {
  my ($self, $path) = @_;

  my ($device_id, $sample_id, @rest) =
    reverse grep { length } splitdir($self->source_dir);

  try {
    open my $fh, '<', $path or croak "Failed to open '$path': $ERRNO";
    my $header = <$fh>;
    close $fh or croak "Failed to close '$path': $ERRNO";

    my ($dev_id) = $header =~ m{device_id=(\S+)\s+}msx;
    if ($dev_id ne $device_id) {
      croak "Within Fastq file '$path' device_id is '$dev_id'";
    }
  } catch {
    $self->error("Failed to read from Fastq file '$path': $_");
  };

  return ($device_id, $sample_id);
}

sub _make_tar_publisher {
  my ($self, $format) = @_;

  # The manifest file has the same name across all sessions of
  # publishing device's output. This means that repeated sessions will
  # incrementally publish the results of the device, or do nothing if
  # it is already completely published.
  my $manifest_path = $self->gridion_run->manifest_file_path($format);

  my $now          = DateTime->now;
  my $now_day      = $now->strftime($ISO8601_DATE);
  my $now_datetime = $now->strftime($ISO8601_DATETIME);

  my $coll = catdir($self->dest_collection, $self->gridion_name,
                    $self->experiment_name, $self->device_id);
  if ($self->irods->is_collection($coll)) {
    $self->debug("Using existing collection '$coll'");
  }
  else {
    $self->debug("Creating new collection '$coll'");
    $self->irods->add_collection($coll);
  }

  my $tar_path = catfile($coll,
                         sprintf '%s_%s_%s',
                         $self->device_id, $format, $now_datetime);
  # Work in the wdir so that the tarred files have the same relative
  # path as if working in the runfolder.
  my $tar_cwd  = $self->wdir->dirname;
  $self->debug("Starting '$format' tar publisher with CWD '$tar_cwd'");

  return WTSI::NPG::HTS::TarPublisher->new
    (manifest_path => $manifest_path,
     remove_files  => 1,
     tar_bytes     => $self->arch_bytes,
     tar_capacity  => $self->arch_capacity,
     tar_cwd       => $tar_cwd,
     tar_path      => $tar_path);
}

sub _close_f5_on_duration {
  my ($self) = @_;

  if ($self->has_f5_publisher) {
    $self->_close_on_duration($self->f5_publisher);
  }

  return;
}

sub _close_fq_on_duration {
  my ($self) = @_;

  if ($self->has_fq_publisher) {
    $self->_close_on_duration($self->fq_publisher);
  }

  return;
}

sub _close_on_duration {
  my ($self, $tar_publisher) = @_;

  if ($tar_publisher->stream_elapsed_time >= $self->arch_duration) {
    $self->info(sprintf q[Closing current tar stream to '%s'; ] .
                q[maximum duration %d sec reached],
                $tar_publisher->tar_stream->tar_file, $self->arch_duration);
    $tar_publisher->close_stream;
  }

  return;
}

sub _close_all {
  my ($self) = @_;

  if ($self->has_f5_publisher) {
    $self->f5_publisher->close_stream;
  }
  if ($self->has_fq_publisher) {
    $self->fq_publisher->close_stream;
  }

  return;
}

sub _build_f5_publisher {
  my ($self) = @_;

  if (not $self->has_device_id) {
    $self->logconfess('Invalid internal state: device_id not set; ',
                      'cannot build a tar publisher');
  }
  return $self->_make_tar_publisher('fast5');
}

sub _build_fq_publisher {
  my ($self) = @_;

  if (not $self->has_device_id) {
    $self->logconfess('Invalid internal state: device_id not set; ',
                      'cannot build a tar publisher');
  }
  return $self->_make_tar_publisher('fastq');
}

sub _build_irods {
  my ($self) = @_;

  return WTSI::NPG::iRODS->new;
}

sub _build_wdir {
  my ($self) = @_;

  return File::Temp->newdir('GridIONRunPublisher.' . $PID . '.XXXXXXXXX',
                            DIR => $self->tmpdir, CLEANUP => 1);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRunPublisher

=head1 DESCRIPTION

Publishes ONT fast5 and fastq files by streaming them through GNU tar
and the tears iRODS client, directly into a series of tar archives in
an iRODS collection.

The fast5 and fastq files must be located under a single top level
directory (the 'source_dir'). Directories and files under the
runfolder are monitored by recursively adding new inotify watches.

The publisher will attempt to capture the sample_id (or "experiment
name" for GridION) and device_id for a run. It does this by examining
the tracking_id group within each Fast5 file. Once the sample_id and
device_id are established, the publisher assumes that they apply to
the entire run.

If Fast5 files are unavailable, the publisher attempts to read the
sample_id and device_id from the path of each Fastq file. Again, once
the device_id is established, the publisher assumes that it applies to
the entire run.

It will write all tar files to 'dest_collection' and add the following
metadata to each:

  'experiment_name'   => GridION experiment name (aka sample_id)
  'device_id'         => GridION device_id
  'dcterms:created'   => Timestamp
  'md5'               => MD5
  'type'              => File suffix

A publishing session is started by calling the 'publish_files' method
which will return when the process is complete. If processing a run
becomes idle (has no inotify events) for longer than session_timeout
seconds, any currently open tar file will be closed and published and
the publish_files method will return, ending the session. Any inotify
watches will be released and no further files will be processed until
publish_files is called again.

If any tar file takes longer to reach its capacity than the
arch_timeout in seconds, that archive is automatically closed. Any
further file(s) will be added to a new tar file.

The publisher will place all tar files will be written in the
specified destination collection. It is the responsibilty of the
calling code to manage which destination collection is used.

When the publish_file method is about to exit, it will enter a
clean-up phase where it will perform a search for all files under
source_dir and attempt to publish every one. If they have already been
published, the underlying TarPublisher will skip them because they
will be present in its manifest.

In a change from previous behaviour, all fastq files are now published
in the clean-up. This is because the ONT basecaller now opens for
writing and consequently closes, each fastq file many hundreds of
times, sending and equal number of inotify events about the incomplete
file. This is a workaround for that behaviour.

Finally, the publisher will add sequencing run ancillary files
(sequencing_summary_n.txt and configuration.cfg) and publisher
tar manifest files to the same iRODS collection.


=head1 BUGS

This class uses inotify to detect when data files to be published are
closed after writing. If files are written before inotify watches are
set up, they will not be detected and published immediately. Instead,
they will be published during the clean-up phase.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017, 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
