package WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor;

use namespace::autoclean;
use DateTime;
use File::Spec::Functions qw[catdir];
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

use WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher;
use WTSI::NPG::HTS::PacBio::Sequel::IsoSeqPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::MonitorBase
       ];

our $VERSION = '';

our $ISO_ANALYSIS = q{(pb_segment_reads_and_isoseq)};

has '+local_staging_area' =>
  (required => 0);

has 'pipeline_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'cromwell.workflows.pb_demux_ccs',
   documentation => 'A specified pipeline name to identify relevant jobs');

has 'task_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'call-lima/execution',
   documentation => 'A specified task name to identify relevant output directories');

has 'job_root' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'cromwell-job',
   documentation => 'Root directory for job processing output');


=head2 publish_analysed_cells

  Arg [1]    : None
  Example    : my ($num_files, $num_published, $num_errors) =
                 $monitor->publish_analysed_cells
  Description: Publish all analysed cells to iRODS. Return the number of
               files, the number published and the number of errors.
  Returntype : Array[Int]

=cut

sub publish_analysed_cells {
  my ($self) = @_;

  my $completed_analysis_jobs = $self->api_client->query_analysis_jobs
      ($self->pipeline_name);

  my ($num_jobs, $num_processed, $num_errors) = (0, 0, 0);

  if (ref $completed_analysis_jobs eq 'ARRAY') {
    my @jobs = @{$completed_analysis_jobs};
    $num_jobs = scalar @jobs;

    foreach my $job (@jobs) {
       try {
         my $analysis_path = $job->{path};
         if(-d $analysis_path){
            $self->debug("Publishing data in analysis job path '$analysis_path'");

            my ($nf, $np, $ne) =
              ($self->pipeline_name =~ m/$ISO_ANALYSIS/smx) ?
              $self->_publish_iso_analysis_path($job) :
              $self->_publish_analysis_path($analysis_path);

            $self->debug("Processed [$np / $nf] files in ",
                       "'$analysis_path' with $ne errors");

            if ($ne > 0) {
               $self->logcroak("Encountered $ne errors while processing ",
                           "[$np / $nf] files in '$analysis_path'");
            }
            $num_processed++;
         }else{
            $self->warn('IGNORING job id ',$job->{id} .
                qq[ as output dir [$analysis_path] not found]);
         }
       } catch {
        $num_errors++;
        $self->error('Failed to process job id ',$job->{id},' cleanly ',
                     "[$num_processed / $num_jobs]: ", $_);
       };
    }
    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed runs processed");
    }
  }
  return ($num_jobs, $num_processed, $num_errors);
}

sub _publish_analysis_path {
  my ($self, $analysis_path) = @_;

  my @glob = glob catdir($analysis_path, $self->job_root, $self->task_name);

  my $runfolder_path = (@glob == 1) ? $glob[0] : q[];

  my @init_args = (irods          => $self->irods,
                   analysis_path  => $analysis_path,
                   runfolder_path => $runfolder_path,
                   mlwh_schema    => $self->mlwh_schema);

  if ($self->dest_collection) {
    push @init_args, dest_collection => $self->dest_collection;
  }

  my $publisher = WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new(@init_args);

  return $publisher->publish_files();
}


sub _publish_iso_analysis_path {
  my ($self, $job) = @_;

  my @glob = glob catdir($job->{path}, $self->task_name);
  my $runfolder_path = (@glob == 1) ? $glob[0] : q[];

  my @init_args = (irods          => $self->irods,
                   analysis_path  => $job->{path},
                   runfolder_path => $runfolder_path,
                   mlwh_schema    => $self->mlwh_schema);

  if ($self->dest_collection) {
    push @init_args, dest_collection => $self->dest_collection;
  }

  push @init_args, analysis_id => $job->{id};

  my $publisher = WTSI::NPG::HTS::PacBio::Sequel::IsoSeqPublisher->new(@init_args);

  return $publisher->publish_files();
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::AnalysisMonitor

=head1 DESCRIPTION

The analysis monitor contacts the PacBio SMRT Link services API 
to get a list of completed analyses and then publishes relevant 
data to iRODS.

It does not query iRODS to find which data have been published
previously, instead the checks are done at the file and metadata
level.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
