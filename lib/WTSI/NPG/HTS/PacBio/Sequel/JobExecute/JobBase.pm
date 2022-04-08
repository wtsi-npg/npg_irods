package WTSI::NPG::HTS::PacBio::Sequel::JobExecute::JobBase;

use namespace::autoclean;
use DateTime;
use File::Spec::Functions qw[catfile];
use Moose::Role;

use WTSI::DNAP::Utilities::Runnable;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'commands4jobs' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   documentation => 'Reference to array of commands');

has 'created_on' =>
  (isa           => 'DateTime',
   is            => 'ro',
   required      => 1,
   documentation => 'Timestamp for log and command files');

has 'identifier' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   predicate     => 'has_identifier',
   documentation => 'An identifier string');

has 'working_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   predicate     => 'has_working_dir',
   documentation => 'The directory for log and command files');

has 'command_file_path' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy_build    => 1,
   builder       => q[_build_command_file_path],
   documentation => 'Path to file containing commands for execution');

sub _build_command_file_path {
  my $self = shift;

  my $cmd = q[mkdir -p ]. $self->working_dir;
  WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
                                       arguments  => ['-c', $cmd])->run;
  if(! -d $self->working_dir ) {
    $self->logcroak('Failed to create '. $self->working_dir);
  }
  my $fp = catfile($self->working_dir,$self->file_prefix .q[.cmds.txt]);
  return $fp;
}

has 'file_prefix' =>
  (isa          => 'Str',
  is            => 'ro',
  lazy_build    => 1,
  builder       => q[_build_file_prefix],
  documentation => 'Consistent filename prefix',);

sub _build_file_prefix {
  my $self = shift;
  return $self->identifier .q[_]. $self->created_on;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::JobExecute::JobBase

=head1 DESCRIPTION

Base for job executors.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2022 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
