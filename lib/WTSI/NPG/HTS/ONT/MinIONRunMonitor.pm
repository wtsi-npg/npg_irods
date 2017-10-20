package WTSI::NPG::HTS::ONT::MinIONRunMonitor;

use namespace::autoclean;

use Data::Dump qw[pp];
use Digest::MD5;
use English qw[-no_match_vars];
use File::Spec::Functions qw[abs2rel catdir catfile splitpath];
use IO::Select;
use Linux::Inotify2;
use Moose;
use MooseX::StrictConstructor;
use Parallel::ForkManager;
use POSIX;
use Try::Tiny;

use WTSI::NPG::HTS::ONT::MinIONRunPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::ArchiveSession
         WTSI::NPG::iRODS::Utilities
       ];

our $VERSION = '';

our $SELECT_TIMEOUT = 2;
our $ISO8601_DAY    = '%Y-%m-%d';

our $DEFAULT_PUBLISHER_LOG_TEMPLATE = << 'LOGCONF'
log4perl.logger = %s, A1

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
   documentation => 'The directory in which MinION runfolders appear');

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

has 'event_queue' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   documentation => 'A queue of Linux::Inotify2::Event objects to be ' .
                    'processed. Inotify callbacks push events here');

sub start {
  my ($self) = @_;

  my $select = IO::Select->new;
  $select->add($self->inotify->fileno);
  my $watch = $self->_start_watch;

  my %in_progress; # Map runfolder path to PID

  $self->info(sprintf
              q[Started MinIONRunMonitor; staging path: '%s', ] .
              q[tar capacity: %d files or %d bytes, tar timeout %d sec ] .
              q[max processes: %d, session timeout %d sec],
              $self->staging_path, $self->arch_capacity, $self->arch_bytes,
              $self->arch_timeout, $self->max_processes,
              $self->session_timeout);

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

  # TODO -- on startup we may find existing runfolders where runs have
  # already begun. We could monitor these automatically to avoid the
  # operator having to touch them.

  my $continue   = 1; # While true, continue monitoring
  my $num_errors = 0;

  # Ensure a clean exit on SIGTERM
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
          # Parent process
          my $abs_path    = $event->fullname;
          my $current_pid = $in_progress{$abs_path};
          if ($current_pid) {
            $self->debug("$abs_path is already being monitored by process ",
                         "with PID $current_pid");
            next EVENT;
          }

          my $log_level = Log::Log4perl->get_logger($self->meta->name)->level;

          my $pid = $pm->start($abs_path) and next EVENT;

          # Child process
          my $child_pid = $PID;
          $self->info("Started MinIONRunPublisher with PID $child_pid on ",
                      "'$abs_path'");
          my $logconf = $self->_make_publisher_logconf($abs_path, $child_pid,
                                                       $log_level);
          Log::Log4perl::init(\$logconf);

          # Publish the data into today's collection
          my $today_coll = DateTime->now->strftime($ISO8601_DAY);
          my $coll = catdir($self->dest_collection, $today_coll);
          $self->info("Publishing to '$coll'");

          my $publisher = WTSI::NPG::HTS::ONT::MinIONRunPublisher->new
            (arch_bytes      => $self->arch_bytes,
             arch_capacity   => $self->arch_capacity,
             arch_timeout    => $self->arch_timeout,
             dest_collection => $coll,
             f5_uncompress   => 0,
             runfolder_path  => $abs_path,
             session_timeout => $self->session_timeout);

          my ($nf, $ne) = $publisher->publish_files;
          my $exit_code = $ne == 0 ? 0 : 1;
          $self->info("Finished publishing $nf files from '$abs_path' ",
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

  if (defined $watch) {
    $watch->cancel;
  }
  $select->remove($self->inotify->fileno);

  return $num_errors;
}

sub _build_inotify {
  my ($self) = @_;

  my $inotify = Linux::Inotify2->new or
    $self->logcroak("Failed to create a new Linux::Inotify2 object: $ERRNO");

  return $inotify;
}

sub _start_watch {
  my ($self) = @_;

  my $path   = $self->staging_path;
  my $events = IN_MOVED_TO | IN_CREATE | IN_MOVED_FROM | IN_DELETE | IN_ATTRIB;
  my $cb     = $self->_make_callback;
  my $watch  = $self->inotify->watch($path, $events, $cb);

  if (defined $watch) {
    $self->debug("Started watch on '$path'");
  }
  else {
    $self->logconfess("Failed to start watch on '$path'");
  }

  return $watch;
}

sub _make_callback {
  my ($self) = @_;

  my $inotify     = $self->inotify;
  my $event_queue = $self->event_queue;

  return sub {
    my $event = shift;

    if ($event->IN_Q_OVERFLOW) {
      $self->warn('Some events were lost!');
    }

    if ($event->IN_CREATE or $event->IN_MOVED_TO or $event->IN_ATTRIB) {
      if ($event->IN_ISDIR) {
        my $path = $event->fullname;

        # Path added was a directory; add the event to the queue, to be
        # handled in the main loop
        $self->debug("Event IN_CREATE/IN_MOVED_TO/IN_ATTRIB on '$path'");
        push @{$event_queue}, $event;
      }
    }

    # Path was removed from the watched hierarchy
    if ($event->IN_DELETE or $event->IN_MOVED_FROM) {
      if ($event->IN_ISDIR) {
        my $path = $event->fullname;
        $self->debug("Event IN_DELETE/IN_MOVED_FROM on '$path'");
      }
    }
  };
};

sub _make_publisher_logconf {
  my ($self, $path, $pid, $level) = @_;

  my $name = $self->meta->name;
  $name =~ s/::/_/gmsx;
  my $logfile = catfile($path, sprintf '%s.%d.log', $name, $pid);

  return sprintf $DEFAULT_PUBLISHER_LOG_TEMPLATE,
    Log::Log4perl::Level::to_level($level), $logfile;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::MinIONRunMonitor

=head1 DESCRIPTION

Uses inotify to monitor a directory for new MinION run
folders. Launches a WTSI::NPG::HTS::ONT::MinIONRunPublisher for each
new run folder detected. A MinIONRunMonitor does not monitor its
directory recursively; that responsibility is delegated to its child
processes. Each child process is responsible for its own subdirectory.

A MinIONRunMonitor will store data in iRODS beneath
dest_collection. To avoid this collection becoming too full,
additional levels of collections will be added beneath dest_collection
to contain the data objects. These are not random, but generated from
a hexdigest of the absolute path of each source directory containing
the data. The left-most 6 characters of the hexdigest will be used to
create three additional levels of collection of the form
<dest_collection>/aa/bb/cc, where the data objects will be stored in
'cc'.

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
