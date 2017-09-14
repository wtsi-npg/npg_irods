package WTSI::NPG::HTS::ONT::GridIONRunMonitor;

use namespace::autoclean;

use Carp;
use Data::Dump q[pp];
use English qw[-no_match_vars];
use File::Spec::Functions qw[catdir catfile rel2abs splitdir];
use IO::Select;
use Linux::Inotify2;
use Moose;
use MooseX::StrictConstructor;
use Parallel::ForkManager;
use POSIX;
use Try::Tiny;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::ArchiveSession
         WTSI::NPG::iRODS::Utilities
       ];

our $VERSION = '';

our $SELECT_TIMEOUT   = 2;

# GridION device directory names match this pattern
our $DEVICE_DIR_REGEX = qr{^GA\d+}msx;

our $DEFAULT_PUBLISHER_LOG_TEMPLATE = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.ONT = INFO, A1
log4perl.logger.WTSI.NPG.HTS.TarPublisher = INFO, A1

log4perl.appender.A1 = Log::Log4perl::Appender::File
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %%d %%-5p %%c - %%m%%n
log4perl.appender.A1.utf8 = 1
log4perl.appender.A1.filename = %s

log4perl.oneMessagePerAppender = 1
LOGCONF
;

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'staging_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The directory in which GridION results appear');

has 'max_processes' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 50,
   documentation => 'The maximum number of child processes to fork');

has 'inotify' =>
  (isa           => 'Linux::Inotify2',
   is            => 'ro',
   required      => 1,
   builder       => '_build_inotify',
   lazy          => 1,
   documentation => 'The inotify instance');

has 'watches' =>
  (isa           => 'HashRef',
   is            => 'rw',
   required      => 1,
   default       => sub { return {} },
   documentation => 'A mapping of absolute paths of watched directories '.
                    'to a corresponding Linux::Inotify2::Watch instance');

has 'watch_history' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   init_arg      => undef,
   documentation => 'All directories watched over the instance lifetime. '.
                    'This is updated automatically by the instance. A ' .
                    'directory will appear multiple times if it is deleted' .
                    'and re-created');

has 'device_dir_queue' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   init_arg      => undef,
   documentation => 'A queue of device directories to be processed');

has 'monitor' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 1,
   documentation => 'While true, continue monitoring the staging_path. ' .
                    'A caller may set this to false in order to stop ' .
                    'monitoring and wait for any child processes to finish');

sub start {
  my ($self) = @_;

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  $self->_start_watches;

  my %in_progress; # Map device directory path to PID

  $self->info(sprintf
              q[Started GridIONRunMonitor; staging path: '%s', ] .
              q[tar capacity: %d files, tar timeout %d sec ] .
              q[max processes: %d, session timeout %d sec],
              $self->staging_path, $self->arch_capacity, $self->arch_timeout,
              $self->max_processes, $self->session_timeout);

  my $pm = Parallel::ForkManager->new($self->max_processes);

  # Use callbacks to track running processes
  $pm->run_on_start(sub {
                      my ($pid, $name) = @_;
                      $self->debug("Process $name (PID $pid) started");
                      $in_progress{$name} = $pid;
                    });
  $pm->run_on_finish(sub {
                       my ($pid, $exit_code, $name) = @_;
                       $self->debug("Process $name (PID $pid) completed " .
                                    "with exit code: $exit_code");
                       delete $in_progress{$name};
                     });

  $pm->run_on_wait(sub { $self->debug('Waiting for a child process...') }, 2);

  my $num_errors = 0;

  try {
    while ($self->monitor) {
      $self->debug('Continue ...');
      if ($select->can_read($SELECT_TIMEOUT)) {
        my $n = $self->inotify->poll;
        $self->debug("$n events");
      }

      if (@{$self->device_dir_queue}) {
        $self->debug(scalar @{$self->device_dir_queue},
                     ' device dirs in queue');

      EVENT: while (my $device_dir = shift @{$self->device_dir_queue}) {
          # Parent process
          my $current_pid = $in_progress{$device_dir};
          if ($current_pid) {
            $self->debug("$device_dir is already being monitored by process ",
                         "with PID $current_pid");
            next EVENT;
          }

          my $pid = $pm->start($device_dir) and next EVENT;

          # Child process
          my $child_pid = $PID;
          $self->info("Started GridIONRunPublisher with PID $child_pid on ",
                      "'$device_dir'");
          my $logconf =
            $self->_make_publisher_logconf($device_dir, $child_pid);
          Log::Log4perl::init(\$logconf);

          # Make $publisher here

          my ($nf, $ne) = (0, 0); # $publisher->publish_files;
          my $exit_code = $ne == 0 ? 0 : 1;
          $self->info("Finished publishing $nf files from '$device_dir' ",
                      "with $ne errors and exit code $exit_code");

          $pm->finish($exit_code);
        }
      }
      else {
        $self->debug("Select timeout ($SELECT_TIMEOUT sec) ...");
        $self->debug('Running processes with PIDs ', pp($pm->running_procs));
        $pm->reap_finished_children;
      }
    }
  } catch {
    $self->error($_);
    $num_errors++;
  };

  $self->_stop_watches;
  $select->remove($self->inotify->fileno);

  my @running = $pm->running_procs;
  $self->info('Waiting for ', scalar @running, ' running processes: ',
              pp(\@running));
  $pm->wait_all_children;
  $self->info('All processes finished');

  return $num_errors;
}

sub _start_watches {
  my ($self) = @_;

  my $events = IN_MOVED_TO | IN_CREATE | IN_MOVED_FROM | IN_DELETE | IN_ATTRIB;
  my $cb     = $self->_make_callback($events);

  my $spath = rel2abs($self->staging_path);
  $self->_start_watch($spath, $events, $cb);

  # Start watches on any existing expt dirs
  my @expt_dirs = $self->_find_dirs($spath);
  foreach my $expt_dir (@expt_dirs) {
    $self->_start_watch($expt_dir, $events, $cb);

    # Queue any existing device dirs
    my @device_dirs = $self->_find_dirs($expt_dir);
    foreach my $device_dir (@device_dirs) {
      push @{$self->device_dir_queue}, $device_dir;
    }
  }

  return;
}

sub _start_watch {
  my ($self, $dir, $events, $callback) = @_;

  $self->debug("Starting watch on '$dir'");
  my $watch;

  -e $dir or
    croak("Invalid directory to watch '$dir'; directory does not exist");
  -d $dir or croak("Invalid directory to watch '$dir'; not a directory");

  if (exists $self->watches->{$dir}) {
    $watch = $self->watches->{$dir};
    $self->debug("Already watching directory '$dir'");
  }
  else {
    $watch = $self->inotify->watch($dir, $events, $callback);
    if ($watch) {
      $self->debug("Started watching directory '$dir'");
      $self->watches->{$dir} = $watch;
      push @{$self->watch_history}, $dir;
    }
    else {
      croak("Failed to start watching directory '$dir': $ERRNO");
    }
  }

  return $watch;
}

sub _stop_watches {
  my ($self) = @_;

  foreach my $dir (keys %{$self->watches}) {
    $self->_stop_watch($dir);
  }

  return $self;
}

sub _stop_watch {
  my ($self, $dir) = @_;

  $self->debug("Stopping watch on '$dir'");
  if (exists $self->watches->{$dir}) {
    $self->watches->{$dir}->cancel;
    delete $self->watches->{$dir};
  }
  else {
    $self->warn("Not watching directory '$dir'; stop request ignored");
  }

  return;
}

# Return a callback to be fired each time an experiment directory is
# added to the staging_path or a device directory is added to an
# experiment directory. For the former, the callback adds itself as an
# event handler and for the latter, the callback pushes the event's
# directory path onto a queue to be handled by the main loop.
sub _make_callback {
  my ($self, $events) = @_;

  my $device_dir_queue = $self->device_dir_queue;

  return sub {
    my $event = shift;

    if ($event->IN_Q_OVERFLOW) {
      $self->warn('Some events were lost!');
    }

    if ($event->IN_ISDIR) {
      my $dir = $event->fullname;

      if ($event->IN_CREATE or $event->IN_MOVED_TO or $event->IN_ATTRIB) {
        if ($self->_is_expt_dir($dir)) {
          # Path is an experiment directory; watch it for new device dirs
          $self->debug("Event on experiment dir '$dir'");
          my $cb = $self->_make_callback($events);
          try {
            $self->_start_watch($dir, $events, $cb);
          } catch {
            $self->error($_);
          };
        }
        elsif ($self->_is_device_dir($dir)) {
          # Path is a device directory; add it to the queue, to be
          # handled in the main loop
          $self->debug("Event on device dir '$dir'");
          $self->debug("Event IN_CREATE/IN_MOVED_TO/IN_ATTRIB on '$dir'");
          push @{$device_dir_queue}, $event->fullpath;
        }
        else {
          # Path is something else
          $self->debug("Ignoring uninteresting directory '$dir'");
        }
      }

      # Path was removed from the watched hierarchy
      if ($event->IN_DELETE or $event->IN_MOVED_FROM) {
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on '$dir'");
        $self->_stop_watch($dir);
      }
    }
  };
};

sub _make_publisher_logconf {
  my ($self, $path, $pid) = @_;

  my $name = $self->meta->name;
  $name =~ s/::/_/gmsx;
  my $logfile = catfile($path, sprintf '%s.%d.log', $name, $pid);

  return sprintf $DEFAULT_PUBLISHER_LOG_TEMPLATE, $logfile;
}

# Return a sorted list of absolute paths of directories within a
# directory
sub _find_dirs {
  my ($self, $dir) = @_;

  opendir my $dh, $dir or croak "Failed to opendir '$dir': $ERRNO";
  my @dirent = grep { ! m{[.]{1,2}$}msx } readdir $dh;
  closedir $dh or $self->warn("Failed to close '$dir': $ERRNO");

  my @dirs = sort grep { -d } map { rel2abs($_, $dir) } @dirent;

  return @dirs;
}

# Return true if the path is an "experiment" directory
sub _is_expt_dir {
  my ($self, $dir) = @_;

  my $abs_dir = rel2abs($dir);
  my @elts    = splitdir($abs_dir);

  my $leaf   = pop @elts;
  my $parent = catdir(@elts);

  return $parent eq $self->staging_path;
}

# Return true if the path is a "device" directory
sub _is_device_dir {
  my ($self, $dir) = @_;

  my $abs_dir = rel2abs($dir);
  my @elts    = splitdir($abs_dir);

  my $leaf      = pop @elts;
  my $parent    = catdir(@elts);
  my $is_device = $leaf =~ $DEVICE_DIR_REGEX and $self->_is_expt_dir($parent);

  return $is_device;
}

sub _build_inotify {
  my ($self) = @_;

  my $inotify = Linux::Inotify2->new or
    $self->logcroak("Failed to create a new Linux::Inotify2 object: $ERRNO");

  return $inotify;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRunMonitor

=head1 DESCRIPTION

Uses inotify to monitor a staging area for new GridION experiment
result directories. Launches a
WTSI::NPG::HTS::ONT::GridIONRunPublisher for each new device directory
detected within the experiment directories. A GridIONRunMonitor does
not monitor directories below a device directory; that responsibility
is delegated to its child processes. Each child process is responsible
for one device directory.

A GridIONRunMonitor will store data in iRODS beneath
dest_collection.

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
