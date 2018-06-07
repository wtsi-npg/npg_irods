package WTSI::NPG::HTS::ONT::GridIONRunMonitor;

use namespace::autoclean;

use Carp;
use Data::Dump q[pp];
use English qw[-no_match_vars];
use File::Path qw[make_path];
use File::Spec::Functions qw[catdir catfile rel2abs splitdir];
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
       ];

our $VERSION = '';

our $ISO8601_DATETIME = '%Y-%m-%dT%H%m%S';

## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
our $SECONDS_PER_MINUTE = 60;
our $MINUTES_PER_HOUR   = 60;
our $HOURS_PER_DAY      = 24;
## use critic

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

has 'devices_active' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   documentation => 'A map of absolute device directory to publisher PID ' .
                    'for active devices');

has 'devices_complete' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   documentation => 'A map of absolute device directory to publisher ' .
                    'completion epoch time for completed device activity');

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

has 'output_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A directory path under which publisher logs and ' .
                    'manifests will be written');

has 'poll_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => $SECONDS_PER_MINUTE,
   documentation => 'The interval in seconds at which to poll the filesystem ' .
                    'for device directories. Defaults to 60 seconds.');

has 'quiet_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => $SECONDS_PER_MINUTE * $MINUTES_PER_HOUR * $HOURS_PER_DAY,
   documentation => 'The interval in seconds after successful completion of ' .
                    'a publisher process during which its device directory ' .
                    'will not be considered for starting a new publisher. ' .
                    'Defaults tp 24 hours');

has 'single_server' =>
  (is            => 'ro',
   isa           => 'Bool',
   default       => 0,
   documentation => 'If true, connect ony a single iRODS server by avoiding ' .
                    'any direct connections to resource servers. This mode ' .
                    'will be much slower to transfer large files, but does ' .
                    'not require resource servers to be accessible');

has 'source_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The directory in which GridION results appear');

has 'tmpdir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   default       => '/tmp',
   documentation => 'Temporary directory for use by publisher processes');

sub start {
  my ($self) = @_;

  $self->info(sprintf
              q[Started GridIONRunMonitor with ] .
              q[staging path: '%s', output dir: '%s', ] .
              q[tar capacity: %d files or %d bytes, tar timeout %d sec, ] .
              q[tar duration: %d, session timeout %d sec, ] .
              q[max processes: %d],
              $self->source_dir, $self->output_dir,
              $self->arch_capacity, $self->arch_bytes, $self->arch_timeout,
              $self->arch_duration, $self->session_timeout,
              $self->max_processes);

  my $pm = Parallel::ForkManager->new($self->max_processes);

  # Use callbacks to track running processes
  $pm->run_on_start(sub {
                      my ($pid, $name) = @_;
                      $self->info("Process $name (PID $pid) started");
                      $self->devices_active->{$name} = $pid;
                    });
  $pm->run_on_finish(sub {
                       my ($pid, $exit_code, $name) = @_;
                       $self->info("Process $name (PID $pid) completed " .
                                   "with exit code: $exit_code");
                       if ($exit_code == 0) {
                         my $now = time;
                         $self->devices_complete->{$name} = $now;
                       }
                       delete ${$self->devices_active}{$name};
                     });

  $pm->run_on_wait(sub {
                     $self->debug('ForkManager waiting for child process...');
                   }, 2);

  my $num_errors = 0;

  while ($self->monitor) {
    try {
      # Parent process
      my @device_dirs = $self->_find_device_dirs;
      $self->debug('Found device directories: ', pp\@device_dirs);

    DEVICE: foreach my $device_dir ($self->_find_device_dirs) {
        # It's active, so we need do nothing
        if (exists ${$self->devices_active}{$device_dir}) {
          my $current_pid = $self->devices_active->{$device_dir};
          $self->debug("'$device_dir' is already being monitored by process ",
                       "with PID $current_pid");
          next DEVICE;
        }

        # If it's not active, this may be caused by a pause in data
        # gathering (flow cells remain viable for days and may be
        # re-started). If the flow cell has been active within the
        # quiet time window, we do not restart it.
        if (exists ${$self->devices_complete}{$device_dir}) {
          my $now            = time;
          my $completed_time = $self->devices_complete->{$device_dir};
          my $elapsed        = $now - $completed_time;
          if ($elapsed <= $self->quiet_interval) {
            $self->debug("'$device_dir' was completed at $completed_time, ",
                         "$elapsed seconds ago. Still in quiet interval.")
          }
          else {
            $self->info("'$device_dir' was completed at $completed_time, ",
                        "$elapsed seconds ago. Quiet interval is passed. ",
                        'Removing from completed jobs to allow re-try');
            delete ${$self->devices_complete}{$device_dir};
          }
          next DEVICE;
        }

        my $log_level = Log::Log4perl->get_logger($self->meta->name)->level;
        my $pid = $pm->start($device_dir) and next DEVICE;

        # Child process
        my $child_pid = $PID;
        $self->info("Started GridIONRunPublisher with PID $child_pid on ",
                    "'$device_dir'");
        my $logconf =
          $self->_make_publisher_logconf($self->output_dir, $child_pid,
                                         $log_level);
        Log::Log4perl::init(\$logconf);

        try {
          my ($expt_name, $device_id) = $self->_parse_device_dir($device_dir);
          my $output_dir = catdir($self->output_dir, $expt_name, $device_id);
          make_path($output_dir);

          my $irods = WTSI::NPG::iRODS->new
            (single_server => $self->single_server);

          my $publisher = WTSI::NPG::HTS::ONT::GridIONRunPublisher->new
            (arch_bytes      => $self->arch_bytes,
             arch_capacity   => $self->arch_capacity,
             arch_duration   => $self->arch_duration,
             arch_timeout    => $self->arch_timeout,
             dest_collection => $self->dest_collection,
             device_id       => $device_id,
             experiment_name => $expt_name,
             f5_uncompress   => 0,
             irods           => $irods,
             output_dir      => $output_dir,
             source_dir      => $device_dir,
             session_timeout => $self->session_timeout,
             tmpdir          => $self->tmpdir);

          my ($nf, $np, $ne) = $publisher->publish_files;
          $self->debug("GridIONRunPublisher returned [$nf, $np, $ne]");

          my $exit_code = $ne == 0 ? 0 : 1;
          $self->info("Finished publishing $nf files from '$device_dir' ",
                      "with $ne errors and exit code $exit_code");

          $pm->finish($exit_code);
        } catch {
          $self->error($_);
          $pm->finish(1);
        };
      }

      $self->debug('ForkManager PIDs: ', pp($pm->running_procs));
      $self->info('In progress: ', pp($self->devices_active));
      $self->info('Completed: ', pp($self->devices_complete));
      $pm->reap_finished_children;

      sleep $self->poll_interval;
    } catch {
      $self->error($_);
      $num_errors++;
    };
  } # while monitor

  my @running = $pm->running_procs;
  $self->info('Waiting for ', scalar @running, ' running processes: ',
              pp(\@running));
  $pm->wait_all_children;
  $self->info('All processes finished');

  return $num_errors;
}

sub _find_device_dirs {
  my ($self) = @_;

  my @device_dirs;

  my $spath = rel2abs($self->source_dir);
  foreach my $dir ($self->_find_dirs($spath)) {
    if ($self->_is_expt_dir($dir)) {

      foreach my $device_dir ($self->_find_dirs($dir)) {
        $self->debug("Found device directory '$device_dir'");
        push @device_dirs, $device_dir;
      }
    }
  }

  return @device_dirs;
}

sub _make_publisher_logconf {
  my ($self, $path, $pid, $level) = @_;

  my $name         = 'WTSI_NPG_HTS_ONT_GridIONRunPublisher';
  my $now_datetime = DateTime->now->strftime($ISO8601_DATETIME);
  my $logfile      = catfile($path, sprintf '%s.%s.%d.log',
                             $name, $now_datetime, $pid);

  my $level_name   = Log::Log4perl::Level::to_level($level);

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

  # Ignore the 'workspace' directory which the instrument happens to
  # create in the source directory.
  return $parent eq $self->source_dir && $leaf ne 'workspace';
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

sub _parse_device_dir {
  my ($self, $dir) = @_;

  if (not $self->_is_device_dir($dir)) {
    croak "Failed to parse '$dir'; is is not a device directory";
  }

  my $abs_dir = rel2abs($dir);
  my @elts = splitdir($abs_dir);

  my $device_id = pop @elts;
  my $expt_name = pop @elts;

  if (not defined $expt_name) {
    croak "Failed to parse an experiment name from '$dir'";
  }
  if (not defined $device_id) {
    croak "Failed to parse a device_id from '$dir'";
  }

  return ($expt_name, $device_id);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONRunMonitor

=head1 DESCRIPTION

Polls a staging area for new GridION experiment result
directories. Launches a WTSI::NPG::HTS::ONT::GridIONRunPublisher for
each existing device directory and for any new device directory
created. A GridIONRunMonitor does not monitor directories below a
device directory; that responsibility is delegated to its child
processes. Each child process is responsible for one device directory.

The filesystem polling interval is set to 60 seconds by default. Once
a publisher completes processing a device directory successfully, no
attempt will be made to process it again for the duration of the quiet
interval, which defaults to 24 hours.

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

A publisher is started when a new device directory is created. It will
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
