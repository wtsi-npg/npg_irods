package WTSI::NPG::HTS::ONT::Watcher;

use Carp;
use English qw[-no_match_vars];
use Linux::Inotify2;
use Moose::Role;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

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

sub start_watch {
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

sub stop_watch {
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

sub stop_watches {
  my ($self) = @_;

  foreach my $dir (keys %{$self->watches}) {
    $self->stop_watch($dir);
  }

  return $self;
}

sub _build_inotify {
  my ($self) = @_;

  my $inotify = Linux::Inotify2->new or
    $self->logcroak("Failed to create a new Linux::Inotify2 object: $ERRNO");

  return $inotify;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::Watcher

=head1 DESCRIPTION

A basic inotify directory watcher.

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
