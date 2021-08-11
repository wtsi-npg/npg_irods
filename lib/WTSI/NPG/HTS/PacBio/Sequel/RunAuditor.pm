package WTSI::NPG::HTS::PacBio::Sequel::RunAuditor;

use namespace::autoclean;
use English qw[-no_match_vars];
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

## no critic (ValuesAndExpressions::ProhibitLeadingZeros)
Readonly::Scalar my $DIR_PERMISSION      => 0770;
Readonly::Scalar my $MODE_OTHER_WRITABLE => 0002;
Readonly::Scalar my $MODE_GROUP_WRITABLE => 0020;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase
       ];

our $VERSION = '';

has 'check_format' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 0,
  documentation => 'check format of runfolder path, false by default',
);

has 'dry_run' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 0,
  documentation => 'dry run mode flag, false by default',
);

=head2 check_runs

  Arg [1]    : None
  Example    : my($num_runs, $num_processed, $num_actioned, $num_errors) =
                 $audit->check_runs;
  Description: Run basic runfolder checks and update permissions if not 
               dry run.
  Returntype : Array[Int]

=cut

sub check_runs {
  my ($self) = @_;

  my ($num_runs, $num_processed, $num_changed, $num_errors) = (0, 0, 0, 0);

  my $completed_runs = $self->api_client->query_runs;

  if (ref $completed_runs eq 'ARRAY') {

    my @runs = @{$completed_runs};
    $num_runs = scalar @runs;

    foreach my $run (@runs) {
      try {
          # only pick up runs that would be picked up for publication
          my $runfolder_path = $self->get_runfolder_path($run);

          my @paths_to_fix;
          if($runfolder_path) {
            if(defined $self->check_format && $self->check_format == 1) {
              $self->valid_runfolder_format($runfolder_path);
            }

            if($self->valid_runfolder_directory($runfolder_path) &&
               $self->_permissions_fixable($runfolder_path)) {
              push @paths_to_fix, $runfolder_path;
            }

            my $publisher  = $self->run_publisher_handle($runfolder_path);
            my $smrt_names = [$publisher->smrt_names];

            foreach my $smrt_name (@{$smrt_names}) {
              my $run_cell_path = $publisher->smrt_path($smrt_name);
              if($self->valid_runfolder_directory($run_cell_path) &&
                 $self->_permissions_fixable($run_cell_path)) {
                push @paths_to_fix, $run_cell_path;
              }
            }
            my $dir_changed = $self->_fix_permissions(\@paths_to_fix);
            if($dir_changed > 0) { $num_changed++ }

            $num_processed++;
          }
        } catch {
            $num_errors++;
            $self->error('Failed to process ',$run->{context},' cleanly');
        };
    }
  }
  return ($num_runs, $num_processed, $num_changed, $num_errors);
}


sub _permissions_fixable {
   my ($self, $directory) = @_;

   my $mode = (stat $directory)[2];

   my $to_be_fixed = 0;
   SWITCH: {
     if ( $mode & $MODE_OTHER_WRITABLE ) {
       $self->warn(qq[Warning: '$directory' dir is other writable]);
     }
     if ( $mode & $MODE_GROUP_WRITABLE ) {
       $self->info(qq[Skipping '$directory' as dir group writable]);
       last SWITCH;
     }
     if (system(q{touch }. $directory) != 0) {
       my $e = $CHILD_ERROR || q[];
       $self->logcroak(qq[Aborting: error $e touching $directory]);
     }

     $self->info(qq[Info: '$directory' requires permissions fix]);
     $to_be_fixed = 1;
   }

   return $to_be_fixed;
}

sub _fix_permissions {
  my ($self, $paths_to_fix) = @_;

  my $dir_changed = 0;
  if(!$self->dry_run && @{$paths_to_fix} >= 1 ) {
    foreach my $directory(@{$paths_to_fix}) {
      if($self->_change_permissions($directory)) { $dir_changed++; }
    }
  }
  return $dir_changed;
}

sub _change_permissions {
  my ($self, $directory) = @_;

  $self->info(qq[Attempting to change permissions on directory '$directory']);

  my $changed = 0;
  if ( chmod $DIR_PERMISSION , $directory ) {
   $self->info(qq[Changed permissions on directory '$directory']);
   $changed = 1;
  }
  else {
    my $e = $ERRNO || q[];
    $self->logcroak(qq[Aborting - error $e changing permissions on $directory]);
  }

  return $changed;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunAuditor

=head1 DESCRIPTION

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
