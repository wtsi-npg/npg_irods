package npg_tracking::daemon::minion_run_monitor;

use File::Spec::Functions;
use Moose;

use npg_tracking::util::types;

use WTSI::NPG::iRODS;

with qw[
         WTSI::NPG::HTS::ArchiveSession
       ];

extends 'npg_tracking::daemon';

our $VERSION = '';

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   builder       => '_build_dest_collection',
   lazy          => 1,
   documentation => 'The destination collection within iRODS to store data');

has 'logconf' =>
  ('is'            => 'ro',
   'isa'           => 'NpgTrackingReadableFile',
   'required'      => 0,
   'predicate'     => 'has_logconf',
   'documentation' => 'Log4perl configuration file');

override 'daemon_name'  => sub {
  return 'npg_minion_run_monitor';
};

override 'command'  => sub {
  my ($self) = @_;

  my @args = ('--collection',      $self->dest_collection,
              '--session-timeout', $self->session_timeout,
              '--arch-capacity',   $self->arch_capacity,
              '--arch-timeout',    $self->arch_timeout);

  if ($self->has_logconf) {
    push @args, '--logconf', $self->logconf;
  }

  return join q[ ], 'npg_minion_run_monitor.pl', @args;
};

override '_build_hosts' => sub {
  return ['sf-nfs-01-01'];
};

sub _build_dest_collection {
  my ($self) = @_;

  my $user      = $ENV{'USER'};
  my $irods     = WTSI::NPG::iRODS->new;
  my $irods_env = $irods->get_irods_env;

  my $dest_collection;
  if ($self->is_prod_user($user) and $irods_env->{irods_zone_name} eq 'seq') {
    $dest_collection = catdir('/seq', 'ont', 'minion');
  }
  else {
    $dest_collection = catdir($irods_env->{irods_home}, 'ont', 'minion');
  }

  return $dest_collection;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

npg_tracking::daemon::minion_run_monitor

=head1 DESCRIPTION

A monitor daemon for MinION staging areas, responsible for launching
processes to stream MinION data into iRODS.

Data will be written to /seq/ont/minion (for a production user) or to
./ont/minion in the user's iRODS home directory (for a non-production
user).

This program is useful only in cases where we need to use a staging
area to marshall data before loading into iRODS e.g. where firewalls
prevent direct access. It is not required in cases where we can stream
to iRODS directly fom the instrument machine.

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
