package  WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase;

use Moose::Role;
use File::Spec::Functions qw[canonpath catdir];

use WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

=head2 run_publisher_handle

  Arg [1]    : runfolder path
  Example    : my $publisher = $self->run_publisher_handle($runfolder_path);
  Description: publisher handle for a specific runfolder
  Returntype : WTSI::NPG::HTS::PacBio::Sequel::RunPublisher

=cut

sub run_publisher_handle {
    my ($self, $runfolder_path) = @_;

    defined $runfolder_path or
        $self->logconfess('A defined runfolder is required');

    $self->debug("Processing data in runfolder path '$runfolder_path'");

    my @init_args = (irods          => $self->irods,
                     runfolder_path => $runfolder_path,
                     mlwh_schema    => $self->mlwh_schema);
    if ($self->api_client) {
        push @init_args, api_client => $self->api_client;
    }
    if ($self->dest_collection) {
        push @init_args, dest_collection => $self->dest_collection;
    }
    my $publisher = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new(@init_args);

    return $publisher;
}


=head2 get_runfolder_path

  Arg [1]    : run information from the webservice
  Example    : my $runfolder_path = $self->get_runfolder_path($runfolder_path);
  Description: Determine the runfolder to load from the webservice result
  Returntype : Str

=cut

sub get_runfolder_path {
  my ($self, $run) = @_;

  defined $run or $self->logconfess('Run information is required');

  my $run_name            = $run->{name};
  my $run_folder          = $run->{context};
  my $num_cells_completed = $run->{numCellsCompleted};
  my $num_cells_failed    = $run->{numCellsFailed};
  my $total_cells         = $run->{totalCells};

  my $runfolder_path;

  SWITCH: {
      if (not ($run_name                    and
               $run_folder                  and
               defined $total_cells         and
               defined $num_cells_completed and
               defined $num_cells_failed
               )) {
        $self->warn('Insufficient information to load run '. pp($run));
        last SWITCH;
      }

      if ($total_cells != ($num_cells_failed + $num_cells_completed)){
        $self->warn('IGNORING ', _run_info($run), ' (Some cells may not be complete)');
        last SWITCH;
      }

      if ($num_cells_completed < 1){
        $self->warn('IGNORING ', _run_info($run), ' (No completed cells to load)');
        last SWITCH;
      }

      my $path = canonpath(catdir($self->local_staging_area, $run_folder));
      if(! -e $path){
          $self->warn('IGNORING ', _run_info($run), ' (Runfolder path not found)');
          last SWITCH;
      }

      $self->info(_run_info($run));
      $runfolder_path = $path;
  }

  return $runfolder_path;
}

sub _run_info {
  my ($run) = @_;

  return sprintf 'Run_name %s Id %s ', $run->{name}, $run->{context};
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase

=head1 DESCRIPTION

Base for run publishing or validation of run publishing.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
