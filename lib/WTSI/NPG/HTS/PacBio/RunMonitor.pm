package WTSI::NPG::HTS::PacBio::RunMonitor;

use namespace::autoclean;
use Data::Dump qw[pp];
use DateTime;
use File::Spec::Functions qw[canonpath catdir catfile splitdir];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;
use URI;

use WTSI::NPG::HTS::PacBio::APIClient;
use WTSI::NPG::HTS::PacBio::RunPublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
       ];

our $VERSION = '';

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::APIClient',
   is            => 'ro',
   required      => 1,
   default       => sub { return WTSI::NPG::HTS::PacBio::APIClient->new },
   documentation => 'A PacBio API client used to fetch job states');

has 'path_uri_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 1,
   default       => undef,
   documentation => 'A regex matching data path URIs to accept');


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

  my $completed_jobs = $self->api_client->query_jobs
    (end_date   => DateTime->now,
     job_status => $WTSI::NPG::HTS::PacBio::APIClient::STATUS_COMPLETE);

  my ($num_jobs, $num_processed, $num_errors) = (0, 0, 0);

  if (ref $completed_jobs eq 'ARRAY') {
    my @jobs = @{$completed_jobs};
    $num_jobs = scalar @jobs;

    foreach my $job (@jobs) {
      try {
        my $runfolder_smrt_path = $self->_get_smrt_path($job);
        if ($runfolder_smrt_path) {
          my ($nf, $np, $ne) = $self->_publish_smrt_path($runfolder_smrt_path);
          $self->debug("Processed [$np / $nf] files in ",
                       "'$runfolder_smrt_path' with $ne errors");

          if ($ne > 0) {
            $self->logcroak("Encountered $ne errors while processing ",
                            "[$np / $nf] files in '$runfolder_smrt_path'");
          }
        }

        $num_processed++;
      } catch {
        $num_errors++;
        $self->error('Failed to process ', _run_info($job), ' cleanly ',
                     "[$num_processed / $num_jobs]: ", $_);
      };
    }

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed jobs processed");
    }
  }

  return ($num_jobs, $num_processed, $num_errors);
}

# Determine the SMRT cell runfolder subdirectory to load from the
# webservice result
sub _get_smrt_path {
  my ($self, $job) = @_;

  my $path_uri                 = $job->{OutputFilePath};
  my $collection_order_perwell = $job->{CollectionOrderPerWell};
  my $collection_number        = $job->{CollectionNumber};
  my $well                     = $job->{Well};
  my $plate                    = $job->{Plate};
  my $index_of_look            = $job->{IndexOfLook};

  my $runfolder_smrt_path;

  SWITCH: {
      if (not ($plate                    and
               $well                     and
               $collection_order_perwell and
               $collection_number        and
               defined $index_of_look)) {
        $self->warn('Insufficient information to load run in: ', pp($job));
        last SWITCH;
      }

      if (not defined $path_uri) {
        $self->info('IGNORING ', _run_info($job),
                    ' (No output path available)');
        last SWITCH;
      }

      if ($self->path_uri_filter) {
        my $regex = $self->path_uri_filter;
        if ($path_uri =~ m{$regex}msx) {
          $self->info('IGNORING ', _run_info($job),
                      " (path URI does not match '$regex')");
          last SWITCH;
        }
      }

      $self->info(_run_info($job));

      # The relative path includes the SMRT cell subdirectory e.g.
      # ./superfoo/46983_1129/F01_1
      my $rel_runfolder_path = URI->new($path_uri)->path;
      my $abs_runfolder_path =
        canonpath(catdir($self->local_staging_area, $rel_runfolder_path));

      $runfolder_smrt_path = $abs_runfolder_path;
    }

  return $runfolder_smrt_path;
}

sub _publish_smrt_path {
  my ($self, $smrt_path) = @_;

  $self->debug("Publishing data in SMRT path '$smrt_path'");

  my @dirs = splitdir($smrt_path);
  my $smrt_name = pop @dirs;
  my $runfolder_path = catdir(@dirs);

  my @init_args = (irods          => $self->irods,
                   runfolder_path => $runfolder_path,
                   mlwh_schema    => $self->mlwh_schema);
  if ($self->dest_collection) {
    push @init_args, dest_collection => $self->dest_collection;
  }

  $self->debug("Publishing data in runfolder '$runfolder_path', ",
               "SMRT name '$smrt_name'");

  my $publisher = WTSI::NPG::HTS::PacBio::RunPublisher->new(@init_args);

  return $publisher->publish_files(smrt_names => [$smrt_name]);
}

sub _run_info {
  my ($job) = @_;

  return sprintf 'Plate %s well %s CollectionOrderPerWell %s ' .
                 'CollectionNumber %s IndexOfLook %d',
                 $job->{OutputFilePath},
                 $job->{Well},
                 $job->{CollectionOrderPerWell},
                 $job->{CollectionNumber},
                 $job->{IndexOfLook};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::RunMonitor

=head1 DESCRIPTION

The run monitor contacts the PacBio instrument web service to
determine which runs have completed and then publishes these runs to
iRODS.

It does not query iRODS to find which runs have been published
previously, instead the checks are done at the file and metadata
level.

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

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
