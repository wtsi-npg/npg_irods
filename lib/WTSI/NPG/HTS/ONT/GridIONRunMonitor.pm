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

use WTSI::NPG::HTS::ONT::GridIONRunPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::iRODS::Utilities
         WTSI::NPG::HTS::ArchiveSession
         WTSI::NPG::HTS::ONT::Watcher
       ];

our $VERSION = '';

our $SELECT_TIMEOUT   = 2;

# GridION device directory names match this pattern
our $DEVICE_DIR_REGEX = qr{^GA\d+}msx;

our $DEFAULT_PUBLISHER_LOG_TEMPLATE = << 'LOGCONF'
log4perl.logger = ERROR, A1

log4perl.logger.WTSI.NPG.HTS.ONT = %s, A1
log4perl.logger.WTSI.NPG.HTS.TarPublisher = %s, A1
log4perl.logger.WTSI.NPG.HTS.TarStream = %s, A1

log4perl.appender.A1 = Log::Log4perl::Appender::File
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %%d %%-5p %%c - %%m%%n
log4perl.appender.A1.utf8 = 1
log4perl.appender.A1.filename = %s

log4perl.oneMessagePerAppender = 1
LOGCONF
;

# Please keep attributes sorted alphabetically
has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'device_dir_queue' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   init_arg      => undef,
   documentation => 'A queue of device directories to be processed');

has 'max_processes' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 50,
   documentation => 'The maximum number of child processes to fork');

has 'monitor' =>
  (isa           => 'Bool',
   is            => 'rw',
   required      => 1,
   default       => 1,
   documentation => 'While true, continue monitoring the source_dir. ' .
                    'A caller may set this to false in order to stop ' .
                    'monitoring and wait for any child processes to finish');

has 'source_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The directory in which GridION results appear');

sub start {
  my ($self) = @_;

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  $self->_start_watches;

  my %in_progress; # Map device directory path to PID

  $self->info(sprintf
              q[Started GridIONRunMonitor; staging path: '%s', ] .
              q[tar capacity: %d files, tar timeout %d sec, ] .
              q[max processes: %d, session timeout %d sec],
              $self->source_dir, $self->arch_capacity, $self->arch_timeout,
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

          my $log_level = Log::Log4perl->get_logger($self->meta->name)->level;

          my $pid = $pm->start($device_dir) and next EVENT;

          # Child process
          my $child_pid = $PID;
          $self->info("Started GridIONRunPublisher with PID $child_pid on ",
                      "'$device_dir'");
          my $logconf =
            $self->_make_publisher_logconf($device_dir, $child_pid,
                                           $log_level);
          Log::Log4perl::init(\$logconf);

          my $publisher = WTSI::NPG::HTS::ONT::GridIONRunPublisher->new
            (arch_bytes      => $self->arch_bytes,
             arch_capacity   => $self->arch_capacity,
             arch_timeout    => $self->arch_timeout,
             dest_collection => $self->dest_collection,
             f5_uncompress   => 0,
             source_dir      => $device_dir,
             session_timeout => $self->session_timeout);

          my ($nf, $np, $ne) = $publisher->publish_files;
          $self->debug("GridIONRunPublisher returned [$nf, $np, $ne]");

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

  $self->stop_watches;
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

  my $spath = rel2abs($self->source_dir);
  $self->start_watch($spath, $events, $cb);

  # Start watches on any existing expt dirs
  my @expt_dirs = $self->_find_dirs($spath);
  foreach my $expt_dir (@expt_dirs) {
    $self->start_watch($expt_dir, $events, $cb);

    # Queue any existing device dirs
    my @device_dirs = $self->_find_dirs($expt_dir);
    foreach my $device_dir (@device_dirs) {
      push @{$self->device_dir_queue}, $device_dir;
    }
  }

  return;
}

# Return a callback to be fired each time an experiment directory is
# added to the source_dir or a device directory is added to an
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
            $self->start_watch($dir, $events, $cb);
          } catch {
            $self->error($_);
          };
        }
        elsif ($self->_is_device_dir($dir)) {
          # Path is a device directory; add it to the queue, to be
          # handled in the main loop
          $self->debug("Event on device dir '$dir'");
          $self->debug("Event IN_CREATE/IN_MOVED_TO/IN_ATTRIB on '$dir'");
          push @{$device_dir_queue}, $dir;
        }
        else {
          # Path is something else
          $self->debug("Ignoring uninteresting directory '$dir'");
        }
      }

      # Path was removed from the watched hierarchy
      if ($event->IN_DELETE or $event->IN_MOVED_FROM) {
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on '$dir'");
        $self->stop_watch($dir);
      }
    }
  };
};

sub _make_publisher_logconf {
  my ($self, $path, $pid, $level) = @_;

  my $name = 'WTSI::NPG::HTS::ONT::GridIONRunPublisher';
  $name =~ s/::/_/gmsx;
  my $logfile = catfile($path, sprintf '%s.%d.log', $name, $pid);
  my $level_name = Log::Log4perl::Level::to_level($level);
  return sprintf $DEFAULT_PUBLISHER_LOG_TEMPLATE,
    $level_name, $level_name, $level_name, $logfile;
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

  return $parent eq $self->source_dir;
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

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRunMonitor

=head1 DESCRIPTION

Uses inotify to monitor a staging area for new GridION experiment
result directories. Launches a
WTSI::NPG::HTS::ONT::GridIONRunPublisher for each existing device
directory and for any new device directory created. A
GridIONRunMonitor does not monitor directories below a device
directory; that responsibility is delegated to its child
processes. Each child process is responsible for one device directory.

A GridIONRunMonitor will store data in iRODS beneath one collection:

<collection>/<gridion hostname>/<experiment name><device id>/

The following scenarios are supported:


1. Pre-existing complete (inactive) and fully published runs

A publisher is started and will time out due to inactivity. On exiting
it will check that all files are published and exit without publishing
any. It will check and update secondary metadata on all files
previously published.


2. Pre-existing complete (inactive) and partially published runs

A publisher is started and will time out due to inactivity. On exiting
it will check that all files are published and exit after publishing
the remainder. It will check and update secondary metadata on all
files published and previously published.


3. Pre-existing incomplete (active) runs

A publisher is started and will first publish new files as it detects
their creation. On exiting it will check that all files are published
and exit after publishing the remainder. It will check and update
secondary metadata on all files published and previously published.


4. Future runs

A publisher is started when a ne device directory is created. It will
first publish new files as it detects their creation. On exiting it
will check that all files are published and exit after publishing any
remainder. It will check and update secondary metadata on all files
published and previouslt published.


=head1 BUGS

This class uses inotify to detect when directories are created and
when data files are closed after writing. If experiment and device
directories are created after the monitor has started up and checked
for existing directories, but before inotify watches are set up, they
will not be detected and their contents will not published.

TODO -- replace the on-startup check for unknown directories with a
periodic check.

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
