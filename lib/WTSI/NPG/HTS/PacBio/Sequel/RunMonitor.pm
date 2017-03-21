package WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;

use namespace::autoclean;
use Data::Dump qw[pp];
use DateTime;
use File::Spec::Functions qw[canonpath catdir catfile splitdir];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
       ];

our $VERSION = '';

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   required      => 1,
   default       => sub { return WTSI::NPG::HTS::PacBio::Sequel::APIClient->new },
   documentation => 'A PacBio Sequel API client used to fetch runs');


=head2 publish_completed_runs

  Arg [1]    : None

  Example    : my ($num_files, $num_published, $num_errors) =
                 $monitor->publish_completed_runs
  Description: Publish all completed runs to iRODS. Return the number of
               files, the number published and the number of errors.
  Returntype : Array[Int]

=cut

sub publish_completed_runs {
  my ($self) = @_;

  my $started_runs = $self->api_client->query_runs;

  my ($num_runs, $num_processed, $num_errors) = (0, 0, 0);

  if (ref $started_runs eq 'ARRAY') {
    my @runs = @{$started_runs};
    $num_runs = scalar @runs;

    foreach my $run (@runs) {
      try {
        my $runfolder_path = $self->_get_runfolder_path($run);

        if ($runfolder_path) {
          my ($nf, $np, $ne) = $self->_publish_runfolder_path($runfolder_path);
          $self->debug("Processed [$np / $nf] files in ",
                       "'$runfolder_path' with $ne errors");

          if ($ne > 0) {
            $self->logcroak("Encountered $ne errors while processing ",
                            "[$np / $nf] files in '$runfolder_path'");
          }
          $num_processed++;
        }
      } catch {
        $num_errors++;
        $self->error('Failed to process ', _run_info($run), ' cleanly ',
                     "[$num_processed / $num_runs]: ", $_);
      };
    }

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed runs processed");
    }
  }

  return ($num_runs, $num_processed, $num_errors);
}

# Determine the runfolder to load from the webservice result
sub _get_runfolder_path {
  my ($self, $run) = @_;

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

sub _publish_runfolder_path {
  my ($self, $runfolder_path) = @_;

  $self->debug("Publishing data in runfolder path '$runfolder_path'");

  my @init_args = (irods          => $self->irods,
                   runfolder_path => $runfolder_path,
                   mlwh_schema    => $self->mlwh_schema);
  if ($self->dest_collection) {
    push @init_args, dest_collection => $self->dest_collection;
  }

  my $publisher = WTSI::NPG::HTS::PacBio::Sequel::RunPublisher->new(@init_args);

  return $publisher->publish_files();
}

sub _run_info {
  my ($run) = @_;

  return sprintf 'Run_name %s Id %s ', $run->{name}, $run->{context};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunMonitor

=head1 DESCRIPTION

The run monitor contacts the PacBio SMRT Link services API to
get a list of runs and then publishes relevant runs to iRODS.

It does not query iRODS to find which runs have been published
previously, instead the checks are done at the file and metadata
level.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>
Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
