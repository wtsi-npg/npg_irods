package WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob;

use namespace::autoclean;
use DateTime;
use File::Slurp qw[write_file];
use File::Spec::Functions qw[catfile];
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::Sequel::JobExecute::JobBase
       ];

our $VERSION = '';


Readonly::Scalar my $DEFAULT_CPUS        => 2;
Readonly::Scalar my $DEFAULT_LIMIT_GROUP => q[pb_irods];
Readonly::Scalar my $DEFAULT_MEMORY      => 4000;
Readonly::Scalar my $DEFAULT_PRIORITY    => 55;
Readonly::Scalar my $DEFAULT_RETRIES     => 2;
Readonly::Scalar my $WR_ENV_LIST_DELIM   => q[,];

Readonly::Array  my @ENV_VARS_TO_PROPAGATE => qw/ PATH
                                                  PERL5LIB
                                                  CLASSPATH
                                                  IRODS_ENVIRONMENT_FILE /;


has 'execution_command' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy_build    => 1,
   builder       => q[_build_execution_command],
   documentation => 'wr execution command');

sub _build_execution_command {
  my $self = shift;

  # Explicitly pass the pipeline's environment to jobs
  my @env_list = ();
  foreach my $var_name (sort @ENV_VARS_TO_PROPAGATE) {
    my $value = $ENV{$var_name};
    if (defined $value && $value ne q[]) {
      push @env_list, "${var_name}=${value}";
    }
  }
  my $stack = join $WR_ENV_LIST_DELIM, @env_list;

  my @common_options = (
    '--cwd'        => '/tmp',
    '--disk'       => 0,
    '--override'   => 2,
    '--retries'    => $DEFAULT_RETRIES,
    '--env'        => q['] . $stack . q['],);

  return join q[ ], qw/wr add/,
    @common_options,
    '-f', $self->command_file_path;
}


sub pre_execute {
  my ($self) = @_;

  $self->_create_command_file();
  if(-f $self->command_file_path && ! -z $self->command_file_path) {
    $self->info(q[Commands file successfully created: ]. $self->command_file_path);
  } else {
    $self->logcroak(q[Error creating commands file: ]. $self->command_file_path);
  }
  return;
}


sub submit {
  my ($self) = @_;

  try {
    my $cmd = $self->execution_command();
    $self->info(qq[Command for execution: $cmd]);
    WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
                                         arguments  => ['-c', $cmd])->run;
    $self->info(q[Commands successfully submitted]);
  } catch {
    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak(pop @stack); # Use a shortened error message
  };
  return;
}


sub _create_command_file {
  my $self = shift;

  my $file_path = $self->command_file_path();
  my $js = JSON->new->canonical;

  my $count = 0; # for logfile naming

  my @cmds;
  foreach my $cmd ( @{$self->commands4jobs} ){
    $count++;
    my $lname   = $self->file_prefix .q[_]. $count .q[.log.txt];
    my $logfile = catfile($self->working_dir, $lname);

    my $params;
    $params->{'cmd'} = join q[ ], q[(umask 0007 &&], $cmd, q[)], q[2>&1],
      q[|], q[tee -a], q["]. $logfile . q["];
    $params->{'memory'}     = $DEFAULT_MEMORY .q[M];
    $params->{'cpus'}       = $DEFAULT_CPUS;
    $params->{'priority'}   = $DEFAULT_PRIORITY;
    $params->{'rep_grp'}    = $self->identifier;
    $params->{'limit_grps'} = [$DEFAULT_LIMIT_GROUP];

    push @cmds, $js->encode($params);
  }
  return write_file($file_path, map { $_ . qq[\n] } @cmds);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob

=head1 DESCRIPTION

Generate commands for wr job submission in the correct format
and write them to a json file. Execute the commands via a wr
manager on the current host using wr add. 

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
